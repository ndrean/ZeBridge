#ifndef ZeBridge_Bridging_Header_h
#define ZeBridge_Bridging_Header_h

#include <stdint.h>

// Zig C ABI declarations for libzb
uint64_t zb_client_open(const char* opts_json);
int      zb_client_close(uint64_t handle);
char*    zb_client_sync(uint64_t h);
char*    zb_client_query(uint64_t h, const char* sql, const char* params_json);
char*    zb_client_mutate(uint64_t h, const char* table, const char* op, const char* key_json, const char* values_json);
char*    zb_client_poll(uint64_t h, uint64_t wait_ms);
char*    zb_client_flush(uint64_t h, uint64_t wait_ms);
void     zb_free(char* p);

#endif /* ZeBridge_Bridging_Header_h */
