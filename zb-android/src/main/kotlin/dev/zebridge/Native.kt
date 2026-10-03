package dev.zebridge

/**
 * libzb's C ABI, one-to-one (libzb/abi.json). Strings cross as UTF-8 byte arrays, never
 * as JNI strings (see zb_jni.c). Internal: apps use [ZeBridge], which owns the thread
 * every call must come from.
 */
internal object Native {
    init {
        System.loadLibrary("zb")
    }

    @JvmStatic external fun abiVersion(): Int
    @JvmStatic external fun lastError(): ByteArray?
    @JvmStatic external fun grammarHash(): ByteArray?
    @JvmStatic external fun createUser(): ByteArray?
    @JvmStatic external fun credsFileText(jwt: ByteArray, seed: ByteArray): ByteArray?
    @JvmStatic external fun connect(opts: ByteArray): Long
    @JvmStatic external fun close(h: Long): Int
    @JvmStatic external fun wipe(h: Long): Int
    @JvmStatic external fun revoked(h: Long): Int
    @JvmStatic external fun wake(h: Long): Int
    @JvmStatic external fun sync(h: Long): ByteArray?
    @JvmStatic external fun poll(h: Long, waitMs: Long): ByteArray?
    @JvmStatic external fun flushOutbox(h: Long, waitMs: Long): ByteArray?
    @JvmStatic external fun query(h: Long, sql: ByteArray, params: ByteArray): ByteArray?
    @JvmStatic external fun mutate(h: Long, table: ByteArray, op: ByteArray, key: ByteArray, values: ByteArray?, version: ByteArray?): ByteArray?
    @JvmStatic external fun stamp(h: Long): ByteArray?
    @JvmStatic external fun join(h: Long, tenant: ByteArray): ByteArray?
    @JvmStatic external fun leave(h: Long, tenant: ByteArray): ByteArray?
    @JvmStatic external fun request(h: Long, subject: ByteArray, payload: ByteArray, timeoutMs: Long): ByteArray?
    @JvmStatic external fun reply(h: Long, id: Long, answer: ByteArray): ByteArray?
    @JvmStatic external fun serve(h: Long, opts: ByteArray): ByteArray?
    @JvmStatic external fun ingest(h: Long, table: ByteArray, answer: ByteArray, scope: ByteArray?): ByteArray?
}
