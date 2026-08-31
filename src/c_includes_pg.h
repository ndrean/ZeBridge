/* Translation unit for the PostgreSQL-only binaries (bridge_sweeper).
 *
 * The sweeper deletes aged tombstones and touches nothing else: no NATS, no
 * msgpack, no compression. src/c_includes.h adds <zstd.h>/<zdict.h> for the
 * bridge's generation chains, and translating them here would make a container
 * that only runs the sweeper carry the zstd HEADERS at build time and the
 * library at runtime — measured before this file existed: bridge_sweeper linked
 * libzstd.1.dylib while referencing zero zstd symbols.
 *
 * ⚠️ Two translation units are fine BECAUSE they are two binaries. The
 * one-unit rule in c_includes.h is about modules inside a single binary sharing
 * one set of C types (two @cImports made two incompatible PGconn types); it is
 * a per-binary invariant, not a per-repo one. Do not "unify" these.
 */
#include <libpq-fe.h>
#include <time.h>
