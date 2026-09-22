/* The C header set when libzb is built WITHOUT the PostgreSQL engine
   (`-Dlibpq=false`): sqlite and zstd only.

   libpq-fe.h is the reason this file exists. It is not in the iOS SDK, and it pulls
   <stdio.h> and the rest of a hosted libc through translate-c, so a phone build that
   can never open a PostgreSQL replica still had to find a PostgreSQL client library
   to compile at all (§10ig). */
#include <sqlite3.h>
#include <zstd.h>
