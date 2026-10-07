// libzb's C ABI — the calls this module makes (libzb/src/capi.zig).
#ifndef ZB_NATIVE_ZB_H
#define ZB_NATIVE_ZB_H
#include <stddef.h>
#include <stdint.h>

uint64_t    zb_client_connect(const char *opts_json);   // 0 on failure: see zb_last_error
int         zb_client_close(uint64_t handle);
char       *zb_client_sync(uint64_t handle);            // JSON, freed with zb_free
char       *zb_client_poll(uint64_t handle, uint64_t wait_ms);
char       *zb_client_query(uint64_t handle, const char *sql, const char *params_json);
char       *zb_client_mutate(uint64_t handle, const char *table, const char *op, const char *key_json, const char *values_json);
char       *zb_client_request(uint64_t handle, const char *subject, const char *payload_json, uint64_t timeout_ms);
char       *zb_client_flush_outbox(uint64_t handle, uint64_t wait_ms);
char       *zb_client_stamp(uint64_t handle);           // {"stamp": …}
int         zb_client_wake(uint64_t handle);            // any thread: ends a poll's wait
int         zb_abi_version(void);                     // libzb/abi.json's version
const char *zb_last_error(void);                        // owned by libzb, not freed
void        zb_free(char *p);
char       *zb_call(const char *fn, const char *args_json); // a pure core rule, JSON in and out

#endif
