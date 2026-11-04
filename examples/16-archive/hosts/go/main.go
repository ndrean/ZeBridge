// The native archiver from Go, through libzbarchive (include/zbarchive.h) and cgo.
//
//	CGO_CFLAGS=-I$PWD/zig-out/include CGO_LDFLAGS=-L$PWD/zig-out/lib go build -o zba-go ./examples/16-archive/hosts/go
//	LD_LIBRARY_PATH=zig-out/lib ./zba-go '{"tenant":"acme","flush_s":60}'
//
// SIGINT/SIGTERM stop it after the chunk being written. Exits with the archiver's code; a Debug
// build of the library that leaked exits 70 instead.
package main

/*
#cgo LDFLAGS: -lzbarchive
#include <stdlib.h>
#include "zbarchive.h"

extern void goLine(void *ctx, uint8_t level, char *line);
static void on_line(void *ctx, uint8_t level, const char *line) { goLine(ctx, level, (char *)line); }
static zba_t *open_with_log(const char *cfg) { return zba_open(cfg, on_line, NULL); }
*/
import "C"

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"unsafe"
)

//export goLine
func goLine(_ unsafe.Pointer, level C.uint8_t, line *C.char) {
	out := os.Stdout
	if level > 0 {
		out = os.Stderr
	}
	fmt.Fprintf(out, "[go host] %s\n", C.GoString(line))
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "usage: %s '<config json>'\n", os.Args[0])
		os.Exit(1)
	}
	if C.zba_abi_version() != 1 {
		fmt.Fprintf(os.Stderr, "libzbarchive ABI %d, this host knows 1\n", C.zba_abi_version())
		os.Exit(1)
	}
	cfg := C.CString(os.Args[1])
	defer C.free(unsafe.Pointer(cfg))
	a := C.open_with_log(cfg)
	if a == nil {
		fmt.Fprintln(os.Stderr, C.GoString(C.zba_last_error()))
		os.Exit(1)
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sig; C.zba_stop(a) }() // zba_stop is safe from any thread
	code := C.zba_run(a)                // blocks this goroutine's thread, not the others
	C.zba_close(a)
	if C.zba_leaks() != 0 {
		os.Exit(70)
	}
	os.Exit(int(code))
}
