package dev.zebridge

import android.content.Context
import org.json.JSONArray
import org.json.JSONObject
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

/** libzb's C ABI version this binding was written for; libzb/python/abi_check.py checks it. */
const val ZB_ABI = 3

/** A call libzb refused, with libzb's own words (`{"error": …}` or `zb_last_error`). */
class ZeBridgeException(message: String) : RuntimeException(message)

/**
 * A ZeBridge client: a local replica that follows PostgreSQL through the bridge, and
 * writes back through it.
 *
 * ```kotlin
 * val zb = ZeBridge(mapOf(
 *     "bridgeUrl" to "https://zb.example.com",
 *     "invite" to code,                    // first run only: what your backend handed the device
 *     "tables" to listOf("orders"),
 * ), context) { result -> /* rows changed: re-query, on your UI thread */ }
 * val rows = zb.query("SELECT * FROM orders WHERE status = ?", "open")
 * zb.mutate("orders", "UPDATE", mapOf("id" to 7), mapOf("status" to "done"))
 * zb.close()
 * ```
 *
 * The same names and options as zb-client-ts, libzb and the Python package (CLIENTS.md).
 * Constructing connects: the first run enrolls with `invite` and keeps the identity
 * next to the replica, later runs need only `bridgeUrl` (or nothing), and the JWT renews
 * itself. With a [context] and neither `dbPath` nor `dbUrl`, the replica is
 * `zebridge.sqlite3` in the app's private database directory, and the identity beside it.
 * Throws [ZeBridgeException] with libzb's reason when it cannot connect.
 *
 * libzb drives one client from one thread; this class owns that thread. Every method may
 * be called from any thread and blocks until the worker has run it — so not from the
 * main thread: a call waits behind the poll in progress, up to [pollMs]. Between calls
 * the worker polls on its own, and [Listener.onPoll] hears about every change.
 */
class ZeBridge @JvmOverloads constructor(
    options: Map<String, Any?>,
    context: Context? = null,
    private val pollMs: Long = 250,
    private val listener: Listener? = null,
) : AutoCloseable {

    /** Called on the client's worker thread; hop to your UI thread to update views. */
    fun interface Listener {
        /** A poll applied rows, settled writes, or brought requests to answer. */
        fun onPoll(result: JSONObject)

        /** The loop hit an error. After a revocation ([revoked] true) it stops for good. */
        fun onError(error: ZeBridgeException) {}
    }

    private val handle: Long
    /** The tenants this principal follows, from the first `sync` (`$KV.tenants.<principal>`). */
    val tenants: List<String>
    private val worker: ScheduledExecutorService

    @Volatile private var workerThread: Thread? = null
    @Volatile private var running = true

    init {
        val abi = Native.abiVersion()
        if (abi != ZB_ABI) throw ZeBridgeException("libzb ABI $abi, this binding speaks $ZB_ABI: rebuild the AAR")
        val opts = JSONObject(options)
        if (context != null && !opts.has("dbPath") && !opts.has("dbUrl")) {
            opts.put("dbPath", context.getDatabasePath("zebridge.sqlite3").also { it.parentFile?.mkdirs() }.path)
        }
        worker = Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "zebridge-${opts.optString("clientId", "client")}").apply { isDaemon = true }
        }
        // Connect AND sync on the worker: the handle belongs to that thread from the start,
        // and zb_last_error, being per thread, is read where the failure happened (an
        // enrollment refused, a bridge unreachable — libzb's words either way).
        val (h, synced) = try {
            worker.submit<Pair<Long, JSONObject>> {
                val h = Native.connect(opts.toString().toByteArray(Charsets.UTF_8))
                if (h == 0L) throw ZeBridgeException(lastErrorText() ?: "zb_client_connect failed")
                val synced = try {
                    result(Native.sync(h))
                } catch (e: ZeBridgeException) {
                    Native.close(h)
                    throw e
                }
                h to synced
            }.get()
        } catch (e: ExecutionException) {
            worker.shutdownNow()
            throw (e.cause as? ZeBridgeException) ?: ZeBridgeException(e.cause?.toString() ?: e.toString())
        }
        handle = h
        tenants = synced.optJSONArray("tenants")?.let { a -> List(a.length()) { a.getString(it) } } ?: emptyList()
        startLoop()
    }

    companion object {
        /** The grammar this library was built for; compare with the bridge's `X-Grammar-Hash`. */
        @JvmStatic
        fun grammarHash(): String = text(Native.grammarHash()) ?: ""

        /**
         * A fresh nkey pair for enrollment: `{"publicKey", "seed"}`. Send `publicKey` to
         * `GET /enroll?code=…&user_pubkey=…`; keep `seed` in the Keystore, never elsewhere.
         */
        @JvmStatic
        fun createUser(): JSONObject = result(Native.createUser())

        /** A `.creds` text from the JWT /enroll returned and the seed kept at [createUser]. */
        @JvmStatic
        fun credsFileText(jwt: String, seed: String): String =
            text(Native.credsFileText(jwt.toByteArray(Charsets.UTF_8), seed.toByteArray(Charsets.UTF_8)))
                ?: throw ZeBridgeException(lastErrorText() ?: "zb_creds_file_text failed")

        private fun text(b: ByteArray?): String? = b?.toString(Charsets.UTF_8)
        private fun lastErrorText(): String? = text(Native.lastError())

        /** A libzb answer: JSON, or an exception carrying libzb's reason. */
        internal fun result(b: ByteArray?): JSONObject {
            val s = text(b) ?: throw ZeBridgeException(lastErrorText() ?: "libzb returned nothing")
            val j = JSONObject(s)
            if (j.has("error")) throw ZeBridgeException(j.opt("error").toString())
            return j
        }
    }

    // ─── the loop ───────────────────────────────────────────────────────────────

    private fun startLoop() {
        worker.execute { workerThread = Thread.currentThread() }
        worker.execute(::loopOnce)
    }

    /**
     * One poll, then the next one queued BEHIND whatever calls arrived meanwhile: the
     * executor runs in order, so a query waits at most one poll.
     */
    private fun loopOnce() {
        if (!running) return
        try {
            val r = result(Native.poll(handle, pollMs))
            if (r.optInt("applied") > 0 || r.optInt("settled") > 0 || (r.optJSONArray("requests")?.length() ?: 0) > 0) {
                listener?.onPoll(r)
            }
            if (running) worker.execute(::loopOnce)
        } catch (e: ZeBridgeException) {
            listener?.onError(e)
            if (Native.revoked(handle) == 1) {
                running = false // for good: every later call answers Revoked
            } else if (running) {
                worker.schedule(::loopOnce, 1, TimeUnit.SECONDS)
            }
        }
    }

    /** Run [block] on the worker, or in place when already there (a Listener calling back in). */
    private fun <T> onWorker(block: () -> T): T {
        if (Thread.currentThread() === workerThread) return block()
        try {
            return worker.submit<T> { block() }.get()
        } catch (e: ExecutionException) {
            throw (e.cause as? ZeBridgeException) ?: ZeBridgeException(e.cause?.toString() ?: e.toString())
        }
    }

    // ─── the client ─────────────────────────────────────────────────────────────

    /**
     * Read the replica: SQL with `?` placeholders. Rows as column → value maps; a
     * write through here is refused (the connection is read-only).
     */
    fun query(sql: String, vararg params: Any?): List<Map<String, Any?>> = onWorker {
        val r = result(Native.query(handle, sql.toByteArray(Charsets.UTF_8), JSONArray(params.toList()).toString().toByteArray(Charsets.UTF_8)))
        val cols = r.getJSONArray("columns")
        val rows = r.getJSONArray("rows")
        List(rows.length()) { i ->
            val row = rows.getJSONArray(i)
            (0 until cols.length()).associate { c -> cols.getString(c) to row.opt(c).let { if (it == JSONObject.NULL) null else it } }
        }
    }

    /**
     * Write a row: applied to the replica at once, sent to PostgreSQL, settled by its
     * verdict ([Listener.onPoll] sees `settled`). [op] is `INSERT`, `UPDATE` or `DELETE`
     * (any case: libzb's core normalises it).
     * [version] pins the write's version instead of the library's clock.
     */
    @JvmOverloads
    fun mutate(table: String, op: String, key: Map<String, Any?>, values: Map<String, Any?>? = null, version: String? = null): JSONObject = onWorker {
        result(Native.mutate(
            handle, table.toByteArray(Charsets.UTF_8), op.toByteArray(Charsets.UTF_8),
            JSONObject(key).toString().toByteArray(Charsets.UTF_8),
            values?.let { JSONObject(it).toString().toByteArray(Charsets.UTF_8) },
            version?.toByteArray(Charsets.UTF_8),
        ))
    }

    /** Send queued writes now, waiting up to [waitMs] for their verdicts. */
    @JvmOverloads
    fun flush(waitMs: Long = 0): JSONObject = onWorker { result(Native.flushOutbox(handle, waitMs)) }

    /** Follow one more tenant (its grants permitting). */
    fun join(tenant: String): JSONObject = onWorker { result(Native.join(handle, tenant.toByteArray(Charsets.UTF_8))) }

    /** Stop following a tenant. */
    fun leave(tenant: String): JSONObject = onWorker { result(Native.leave(handle, tenant.toByteArray(Charsets.UTF_8))) }

    /** Ask a tenant's service (`query.<tenant>.<name>`); answer rows come back in the reply. */
    @JvmOverloads
    fun request(subject: String, payload: JSONObject = JSONObject(), timeoutMs: Long = 5000): JSONObject = onWorker {
        result(Native.request(handle, subject.toByteArray(Charsets.UTF_8), payload.toString().toByteArray(Charsets.UTF_8), timeoutMs))
    }

    /** Store a [request]'s answer rows in an on-demand table. */
    @JvmOverloads
    fun ingest(table: String, answer: JSONObject, scope: JSONObject? = null): JSONObject = onWorker {
        result(Native.ingest(handle, table.toByteArray(Charsets.UTF_8), answer.toString().toByteArray(Charsets.UTF_8), scope?.toString()?.toByteArray(Charsets.UTF_8)))
    }

    /** Answer requests as a service; they arrive in [Listener.onPoll]'s `requests`. */
    fun serve(options: JSONObject): JSONObject = onWorker { result(Native.serve(handle, options.toString().toByteArray(Charsets.UTF_8))) }

    /** Reply to request [id] from [Listener.onPoll]. */
    fun reply(id: Long, answer: JSONObject): JSONObject = onWorker { result(Native.reply(handle, id, answer.toString().toByteArray(Charsets.UTF_8))) }

    /** True once the operator revoked this principal. The rows stay; [wipe] removes them. */
    val revoked: Boolean get() = Native.revoked(handle) == 1

    /** Stop, close, and delete the replica's files — the application's explicit act. */
    fun wipe() = shutdown { Native.wipe(handle) }

    /** Stop the loop and close the replica. Safe to call twice. */
    override fun close() = shutdown { Native.close(handle) }

    private fun shutdown(end: () -> Int) {
        if (!running && worker.isShutdown) return
        running = false
        try {
            onWorker { end() }
        } finally {
            worker.shutdown()
        }
    }
}
