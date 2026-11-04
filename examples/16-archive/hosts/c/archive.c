/* The native archiver from C, through libzbarchive (include/zbarchive.h).
 *
 *   cc -I zig-out/include examples/16-archive/hosts/c/archive.c -L zig-out/lib -lzbarchive -o zba-c
 *   LD_LIBRARY_PATH=zig-out/lib ./zba-c '{"tenant":"acme","flush_s":60}'
 *
 * SIGINT/SIGTERM stop it after the chunk being written. Exits with the archiver's code; a Debug
 * build of the library that leaked exits 70 instead. */
#include <signal.h>
#include <stdio.h>
#include <string.h>

#include "zbarchive.h"

static zba_t *archive;

static void on_signal(int sig) {
    (void)sig;
    zba_stop(archive); /* one atomic store: safe in a handler */
}

static void on_line(void *ctx, uint8_t level, const char *line) {
    fprintf(level ? stderr : stdout, "[c host] %s\n", line);
    fflush(level ? stderr : stdout);
    (void)ctx;
}

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s '<config json>'\n", argv[0]);
        return 1;
    }
    if (zba_abi_version() != 1) {
        fprintf(stderr, "libzbarchive ABI %d, this host knows 1\n", zba_abi_version());
        return 1;
    }
    archive = zba_open(argv[1], on_line, NULL);
    if (!archive) {
        fprintf(stderr, "%s\n", zba_last_error());
        return 1;
    }
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    int code = zba_run(archive);
    zba_close(archive);
    if (zba_leaks() != 0) return 70;
    return code;
}
