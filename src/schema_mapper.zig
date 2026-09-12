const std = @import("std");

/// A compile-time perfect hash map for O(1) PostgreSQL to SQLite type translation
pub const sqlite_type_map = std.StaticStringMap([]const u8).initComptime(.{
    .{ "integer", "INTEGER" },
    .{ "int", "INTEGER" },
    .{ "int4", "INTEGER" },
    .{ "bigint", "INTEGER" },
    .{ "int8", "INTEGER" },
    .{ "smallint", "INTEGER" },
    .{ "int2", "INTEGER" },
    .{ "serial", "INTEGER" },
    .{ "serial4", "INTEGER" },
    .{ "bigserial", "INTEGER" },
    .{ "serial8", "INTEGER" },
    .{ "smallserial", "INTEGER" },
    .{ "serial2", "INTEGER" },
    .{ "boolean", "INTEGER" },
    .{ "bool", "INTEGER" },
    // Genuine binary floats: PostgreSQL float4/float8 and SQLite REAL are both
    // IEEE-754, and the CDC path sends them as .float64 → a JSON/msgpack number.
    .{ "real", "REAL" },
    .{ "float4", "REAL" },
    .{ "double precision", "REAL" },
    .{ "float8", "REAL" },

    // NUMERIC/DECIMAL are arbitrary-precision and must NOT become REAL: SQLite's
    // REAL is float64, so 20,8 money silently loses digits. They also arrive over
    // CDC as .numeric → a *string* (e.g. "123.4500"), so declaring REAL would make
    // the published schema disagree with the published data. TEXT keeps both exact
    // and consistent; SQLite has no decimal type, so the client does the arithmetic.
    .{ "numeric", "TEXT" },
    .{ "decimal", "TEXT" },

    // §10ex: bytes. bytea, and the PostGIS types, which PostgreSQL sends as EWKB
    // bytes in binary mode — the bridge carries them as msgpack `bin` and never
    // looks inside; the client that renders a map decodes EWKB itself.
    .{ "bytea", "BLOB" },
    .{ "geometry", "BLOB" },
    .{ "geography", "BLOB" },
    // §10fg: pgvector, normalised on the wire to what sqlite-vec reads (little-endian,
    // no header), and bit(n) as its packed bytes. bit varying stays TEXT ('0101').
    .{ "vector", "BLOB" },
    .{ "halfvec", "BLOB" },
    .{ "sparsevec", "BLOB" },
    .{ "bit", "BLOB" },
});

/// Translates a PostgreSQL type to an SQLite type
/// Defaults to "TEXT" for types like character varying, timestamp, jsonb, arrays, etc.
pub fn pgToSqliteType(pg_type: []const u8) []const u8 {
    // `format_type` spells modifiers — `numeric(20,8)`, `geometry(Point,4326)`,
    // `character varying(255)` — and the map is keyed by the bare name.
    const bare = if (std.mem.indexOfScalar(u8, pg_type, '(')) |i| pg_type[0..i] else pg_type;
    return sqlite_type_map.get(bare) orelse "TEXT";
}

test "pgToSqliteType - bytes are BLOB, modifiers do not hide the type" {
    try std.testing.expectEqualStrings("BLOB", pgToSqliteType("bytea"));
    try std.testing.expectEqualStrings("BLOB", pgToSqliteType("geometry"));
    try std.testing.expectEqualStrings("BLOB", pgToSqliteType("geometry(Point,4326)"));
    try std.testing.expectEqualStrings("BLOB", pgToSqliteType("geography"));
    try std.testing.expectEqualStrings("TEXT", pgToSqliteType("numeric(20,8)"));
    try std.testing.expectEqualStrings("TEXT", pgToSqliteType("bytea[]"));
    try std.testing.expectEqualStrings("BLOB", pgToSqliteType("vector(1536)"));
    try std.testing.expectEqualStrings("BLOB", pgToSqliteType("halfvec(3)"));
    try std.testing.expectEqualStrings("BLOB", pgToSqliteType("sparsevec(5)"));
    try std.testing.expectEqualStrings("BLOB", pgToSqliteType("bit(8)"));
    try std.testing.expectEqualStrings("TEXT", pgToSqliteType("bit varying(12)"));
}

test "pgToSqliteType - exact types must not become REAL" {
    // NUMERIC/DECIMAL are arbitrary-precision in Postgres and arrive over CDC as
    // strings. Mapping them to SQLite REAL (float64) silently loses digits on money
    // and contradicts the delivered payload, so they must map to TEXT.
    try std.testing.expectEqualStrings("TEXT", pgToSqliteType("numeric"));
    try std.testing.expectEqualStrings("TEXT", pgToSqliteType("decimal"));
}

test "pgToSqliteType - binary floats stay REAL" {
    try std.testing.expectEqualStrings("REAL", pgToSqliteType("real"));
    try std.testing.expectEqualStrings("REAL", pgToSqliteType("float4"));
    try std.testing.expectEqualStrings("REAL", pgToSqliteType("float8"));
    try std.testing.expectEqualStrings("REAL", pgToSqliteType("double precision"));
}

test "pgToSqliteType - integer family and booleans" {
    for ([_][]const u8{ "integer", "int4", "bigint", "int8", "smallint", "serial", "bigserial" }) |t| {
        try std.testing.expectEqualStrings("INTEGER", pgToSqliteType(t));
    }
    // SQLite has no boolean; 0/1 in an INTEGER column is the conventional encoding.
    try std.testing.expectEqualStrings("INTEGER", pgToSqliteType("boolean"));
}

test "pgToSqliteType - unknown and text-ish types fall back to TEXT" {
    // information_schema reports NUMERIC(20,8) as plain "numeric", but reports many
    // others verbatim; anything unmapped must degrade to TEXT rather than guess.
    try std.testing.expectEqualStrings("TEXT", pgToSqliteType("timestamp with time zone"));
    try std.testing.expectEqualStrings("TEXT", pgToSqliteType("jsonb"));
    try std.testing.expectEqualStrings("TEXT", pgToSqliteType("ARRAY"));
    try std.testing.expectEqualStrings("TEXT", pgToSqliteType("uuid"));
    try std.testing.expectEqualStrings("TEXT", pgToSqliteType("some_custom_enum"));
}
