# Engine headers

The C headers of the two optional storage engines, copied unchanged so that libzb
compiles both engines on any machine, phones included. libzb does not link either
library: it opens it at run time, only when a client asks for that engine
(`src/engines.zig`, DISTRIBUTION.md).

| header | from | license |
| --- | --- | --- |
| `duckdb.h` | DuckDB 1.5.5 | MIT, `LICENSE.duckdb` |
| `libpq-fe.h`, `postgres_ext.h` | PostgreSQL 18.6 (libpq) | PostgreSQL License, `COPYRIGHT.postgresql` |

Updating: copy the new headers over these and rebuild. A function libzb calls that the
installed library lacks is reported by name when the engine opens.
