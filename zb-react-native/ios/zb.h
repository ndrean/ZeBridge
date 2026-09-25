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
int         zb_abi_version(void);                     // libzb/abi.json's version
const char *zb_last_error(void);                        // owned by libzb, not freed
void        zb_free(char *p);

// Streaming zstd (plain frames): push compressed bytes, get what they inflate to.
void       *zb_zstd_new(void);
void        zb_zstd_free(void *ctx);
int         zb_zstd_push(void *ctx, const uint8_t *src, size_t len, uint8_t **out, size_t *out_len);

#endif
