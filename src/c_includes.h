/* Translation unit for all C headers the bridge needs.
 *
 * Consumed by the `addTranslateC` step in build.zig, which turns it into a real
 * Zig module named "c". Keeping every C header in one translation unit means all
 * Zig modules share one set of C types — with @cImport, each import site produced
 * its own namespace and the same PGconn from two modules were distinct types.
 */
#include <libpq-fe.h>
#include <time.h>
