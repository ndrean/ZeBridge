/*
 * libzbarchive — the ZeBridge readings archiver as a C library.
 *
 * One tenant's sensor readings, from the NATS stream READINGS_<tenant> into Parquet files in
 * S3 (Cloudflare R2, AWS, SeaweedFS, MinIO), each file recorded in the archive manifest
 * (NATS KV time_manifests, key <tenant>.readings) only after its upload is verified. The same
 * archive, rules and exit codes as the `bridge_archiver` command and the Python package
 * zb-archiver (zb-archiver/README.md). Needs libduckdb at run time (ZB_DUCKDB_LIB, else the
 * loader's search); no DuckDB extension, no AWS SDK.
 *
 *     zba_t *a = zba_open("{\"tenant\":\"acme\",\"flush_s\":60}", NULL, NULL);
 *     if (!a) { fprintf(stderr, "%s\n", zba_last_error()); return 1; }
 *     int code = zba_run(a);            // blocks; zba_stop(a) from another thread or a signal
 *     zba_close(a);
 *
 * Configuration (JSON, every key optional but "tenant"; an unknown key is refused):
 *   tenant, flush_s (60), batch (100000), outage_s (3600), exit_when_idle_s (0 = never),
 *   accept_loss (false), nats_url, creds, tls_ca, s3_endpoint, s3_bucket, s3_region,
 *   access_key_id, secret_access_key, work_dir.
 * An unset string is read from the environment: NATS_URL, NATS_CREDS, NATS_TLS_CA,
 * S3_ENDPOINT, S3_BUCKET, S3_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, TMPDIR.
 */
#ifndef ZBARCHIVE_H
#define ZBARCHIVE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct zba zba_t;

/* level: 0 info, 1 warning, 2 error. `line` is valid for the call only. */
typedef void (*zba_log_fn)(void *ctx, uint8_t level, const char *line);

enum {
    ZBA_STOPPED = 0,          /* zba_stop, or exit_when_idle_s */
    ZBA_FAILED = 1,           /* a failure, said in the log (DuckDB missing, NATS, S3, …) */
    ZBA_RETENTION_SHORT = 2,  /* the stream keeps readings for less than 2 × flush + outage */
    ZBA_MANIFEST_MOVED = 3,   /* another archiver wrote the manifest: one writer per tenant */
    ZBA_READINGS_LOST = 4,    /* readings left the stream before any file held them */
    ZBA_UPLOAD_MISMATCH = 5   /* the object in S3 does not have the size written */
};

/* 1 for this header. A host checks it before the first call. */
int zba_abi_version(void);

/* A handle, or NULL with zba_last_error() set. `log` may be NULL: lines go to stdout. */
zba_t *zba_open(const char *config_json, zba_log_fn log, void *ctx);

/* Why the last zba_open on this thread failed. */
const char *zba_last_error(void);

/* Blocks until the archive stops; returns one of the codes above. */
int zba_run(zba_t *a);

/* Asks a running zba_run to return. Safe from any thread and from a signal handler. */
void zba_stop(zba_t *a);

/* Frees the handle; not while zba_run runs on it. */
void zba_close(zba_t *a);

/* Handles closed with a memory leak (Debug and ReleaseSafe builds check; ReleaseFast does
 * not and returns 0). A host's test asserts 0 against a Debug build. */
uint32_t zba_leaks(void);

#ifdef __cplusplus
}
#endif

#endif
