//! The `duckdb` import when libzb is built WITHOUT the DuckDB engine (`-Dduckdb=false`,
//! the default and the phone build): every DuckDB code path is behind
//! `build_options.duckdb`, so nothing here is ever referenced — the module only has
//! to exist for `@import("duckdb")` to resolve.
