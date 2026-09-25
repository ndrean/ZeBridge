// libzb's C ABI — the calls this module makes (libzb/src/capi.zig).
#ifndef ZB_NATIVE_ZB_H
#define ZB_NATIVE_ZB_H
#include <stdint.h>

uint64_t    zb_client_connect(const char *opts_json);   // 0 on failure: see zb_last_error
int         zb_client_close(uint64_t handle);
char       *zb_client_sync(uint64_t handle);            // JSON, freed with zb_free
char       *zb_client_poll(uint64_t handle, uint64_t wait_ms);
char       *zb_client_query(uint64_t handle, const char *sql, const char *params_json);
const char *zb_last_error(void);                        // owned by libzb, not freed
void        zb_free(char *p);

#endif
