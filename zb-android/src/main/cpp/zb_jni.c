/* The JNI layer between dev.zebridge.Native and libzb's C ABI (libzb/abi.json).
 *
 * Strings cross as byte[] holding UTF-8, never as jstring: JNI's string functions speak
 * "modified UTF-8", which encodes an emoji (any character outside the BMP) as two
 * surrogates and NUL as two bytes. libzb reads and writes standard UTF-8, so a row
 * holding an emoji would be corrupted one way or crash NewStringUTF the other. Kotlin
 * encodes and decodes with Charsets.UTF_8 instead.
 *
 * Every function here is a thin copy: bytes in → a NUL-terminated C string, a returned
 * C string → bytes out, then zb_free. A NULL result stays null; the Kotlin side reads
 * zb_last_error on the same thread (it is per thread, like errno). */
#include <jni.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

extern int zb_abi_version(void);
extern void zb_free(char *p);
extern char *zb_call(const char *fn, const char *args_json);
extern const char *zb_last_error(void);
extern char *zb_grammar_hash(void);
extern char *zb_create_user(void);
extern char *zb_creds_file_text(const char *jwt, const char *seed);
extern uint64_t zb_client_connect(const char *opts_json);
extern int zb_client_close(uint64_t h);
extern int zb_client_wipe(uint64_t h);
extern int zb_client_revoked(uint64_t h);
extern char *zb_client_sync(uint64_t h);
extern char *zb_client_stamp(uint64_t h);
extern int zb_client_wake(uint64_t h);
extern char *zb_client_poll(uint64_t h, uint64_t wait_ms);
extern char *zb_client_query(uint64_t h, const char *sql, const char *params_json);
extern char *zb_client_mutate(uint64_t h, const char *table, const char *op, const char *key_json, const char *values_json);
extern char *zb_client_mutate_at(uint64_t h, const char *table, const char *op, const char *key_json, const char *values_json, const char *version);
extern char *zb_client_flush_outbox(uint64_t h, uint64_t wait_ms);
extern char *zb_client_join(uint64_t h, const char *tenant);
extern char *zb_client_leave(uint64_t h, const char *tenant);
extern char *zb_client_request(uint64_t h, const char *subject, const char *payload_json, uint64_t timeout_ms);
extern char *zb_client_reply(uint64_t h, uint64_t id, const char *answer_json);
extern char *zb_client_serve(uint64_t h, const char *opts_json);
extern char *zb_client_ingest(uint64_t h, const char *table, const char *answer_json, const char *scope_json);

/* byte[] (UTF-8) → a malloc'd NUL-terminated copy; null stays NULL. */
static char *in(JNIEnv *env, jbyteArray a) {
    if (a == NULL) return NULL;
    jsize n = (*env)->GetArrayLength(env, a);
    char *s = malloc((size_t)n + 1);
    if (s == NULL) return NULL;
    (*env)->GetByteArrayRegion(env, a, 0, n, (jbyte *)s);
    s[n] = 0;
    return s;
}

static jbyteArray bytes(JNIEnv *env, const char *s) {
    if (s == NULL) return NULL;
    jsize n = (jsize)strlen(s);
    jbyteArray a = (*env)->NewByteArray(env, n);
    if (a != NULL) (*env)->SetByteArrayRegion(env, a, 0, n, (const jbyte *)s);
    return a;
}

/* A string libzb returned and the caller owns: copied out, then freed. */
static jbyteArray owned(JNIEnv *env, char *s) {
    jbyteArray a = bytes(env, s);
    if (s != NULL) zb_free(s);
    return a;
}

#define FN(ret, name) JNIEXPORT ret JNICALL Java_dev_zebridge_Native_##name
#define U64(x) ((uint64_t)(x))

FN(jint, abiVersion)(JNIEnv *env, jclass cls) { return zb_abi_version(); }
FN(jbyteArray, lastError)(JNIEnv *env, jclass cls) { return bytes(env, zb_last_error()); }
FN(jbyteArray, grammarHash)(JNIEnv *env, jclass cls) { return owned(env, zb_grammar_hash()); }
FN(jbyteArray, createUser)(JNIEnv *env, jclass cls) { return owned(env, zb_create_user()); }

FN(jbyteArray, call)(JNIEnv *env, jclass cls, jbyteArray fn, jbyteArray args) {
    char *f = in(env, fn), *a = in(env, args);
    jbyteArray r = owned(env, zb_call(f, a));
    free(f); free(a);
    return r;
}

FN(jbyteArray, credsFileText)(JNIEnv *env, jclass cls, jbyteArray jwt, jbyteArray seed) {
    char *j = in(env, jwt), *s = in(env, seed);
    jbyteArray r = owned(env, zb_creds_file_text(j, s));
    free(j);
    if (s != NULL) { memset(s, 0, strlen(s)); free(s); } /* the private half: do not leave it in the heap */
    return r;
}

FN(jlong, connect)(JNIEnv *env, jclass cls, jbyteArray opts) {
    char *o = in(env, opts);
    uint64_t h = zb_client_connect(o);
    if (o != NULL) { memset(o, 0, strlen(o)); free(o); } /* may hold creds text */
    return (jlong)h;
}

FN(jint, close)(JNIEnv *env, jclass cls, jlong h) { return zb_client_close(U64(h)); }
FN(jint, wipe)(JNIEnv *env, jclass cls, jlong h) { return zb_client_wipe(U64(h)); }
FN(jint, revoked)(JNIEnv *env, jclass cls, jlong h) { return zb_client_revoked(U64(h)); }
/* The one call allowed from any thread: ends the worker's poll wait. */
FN(jint, wake)(JNIEnv *env, jclass cls, jlong h) { return zb_client_wake(U64(h)); }
FN(jbyteArray, sync)(JNIEnv *env, jclass cls, jlong h) { return owned(env, zb_client_sync(U64(h))); }
FN(jbyteArray, poll)(JNIEnv *env, jclass cls, jlong h, jlong wait_ms) { return owned(env, zb_client_poll(U64(h), U64(wait_ms))); }
FN(jbyteArray, flushOutbox)(JNIEnv *env, jclass cls, jlong h, jlong wait_ms) { return owned(env, zb_client_flush_outbox(U64(h), U64(wait_ms))); }

FN(jbyteArray, query)(JNIEnv *env, jclass cls, jlong h, jbyteArray sql, jbyteArray params) {
    char *q = in(env, sql), *p = in(env, params);
    jbyteArray r = owned(env, zb_client_query(U64(h), q, p));
    free(q); free(p);
    return r;
}

FN(jbyteArray, mutate)(JNIEnv *env, jclass cls, jlong h, jbyteArray table, jbyteArray op, jbyteArray key, jbyteArray values, jbyteArray version) {
    char *t = in(env, table), *o = in(env, op), *k = in(env, key), *v = in(env, values), *ver = in(env, version);
    char *res = ver != NULL ? zb_client_mutate_at(U64(h), t, o, k, v, ver) : zb_client_mutate(U64(h), t, o, k, v);
    jbyteArray r = owned(env, res);
    free(t); free(o); free(k); free(v); free(ver);
    return r;
}

FN(jbyteArray, stamp)(JNIEnv *env, jclass cls, jlong h) { return owned(env, zb_client_stamp(U64(h))); }

FN(jbyteArray, join)(JNIEnv *env, jclass cls, jlong h, jbyteArray tenant) {
    char *t = in(env, tenant);
    jbyteArray r = owned(env, zb_client_join(U64(h), t));
    free(t);
    return r;
}

FN(jbyteArray, leave)(JNIEnv *env, jclass cls, jlong h, jbyteArray tenant) {
    char *t = in(env, tenant);
    jbyteArray r = owned(env, zb_client_leave(U64(h), t));
    free(t);
    return r;
}

FN(jbyteArray, request)(JNIEnv *env, jclass cls, jlong h, jbyteArray subject, jbyteArray payload, jlong timeout_ms) {
    char *s = in(env, subject), *p = in(env, payload);
    jbyteArray r = owned(env, zb_client_request(U64(h), s, p, U64(timeout_ms)));
    free(s); free(p);
    return r;
}

FN(jbyteArray, reply)(JNIEnv *env, jclass cls, jlong h, jlong id, jbyteArray answer) {
    char *a = in(env, answer);
    jbyteArray r = owned(env, zb_client_reply(U64(h), U64(id), a));
    free(a);
    return r;
}

FN(jbyteArray, serve)(JNIEnv *env, jclass cls, jlong h, jbyteArray opts) {
    char *o = in(env, opts);
    jbyteArray r = owned(env, zb_client_serve(U64(h), o));
    free(o);
    return r;
}

FN(jbyteArray, ingest)(JNIEnv *env, jclass cls, jlong h, jbyteArray table, jbyteArray answer, jbyteArray scope) {
    char *t = in(env, table), *a = in(env, answer), *s = in(env, scope);
    jbyteArray r = owned(env, zb_client_ingest(U64(h), t, a, s));
    free(t); free(a); free(s);
    return r;
}
