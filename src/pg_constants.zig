const std = @import("std");

/// PostgreSQL built-in Type OIDs (Object IDs).
///
/// These values are standard and obtained from PostgreSQL with: `docker exec postgres psql -U postgres -d postgres -c "SELECT oid, typname FROM pg_type WHERE typname IN ('bool', 'int2', 'int4', 'int8', 'float4', 'float8', 'text', 'varchar', 'bpchar', 'date', 'timestamptz', 'uuid', 'bytea', 'jsonb', 'numeric', '_int4', '_text', '_jsonb') ORDER BY oid;"`
pub const PgOid = enum(u32) {
    // Numeric Types
    BOOL = 16,
    BYTEA = 17,
    INT2 = 21, // smallint
    INT4 = 23, // integer
    INT8 = 20, // bigint
    FLOAT4 = 700,
    FLOAT8 = 701,

    // Character Types
    TEXT = 25,
    VARCHAR = 1043,
    BPCHAR = 1042, // char(n)

    // Date/Time Types
    DATE = 1082,
    TIMESTAMP = 1114, // timestamp without time zone (8 bytes)
    TIMESTAMPTZ = 1184, // timestamp with time zone (8 bytes)

    // Others
    UUID = 2950,
    JSON = 114, // json (text format, before jsonb)
    JSONB = 3802,
    NUMERIC = 1700,

    // Array Types (prefixed with underscore in PostgreSQL)
    ARRAY_INT2 = 1005, // _int2 (smallint[])
    ARRAY_INT4 = 1007, // _int4 (integer[])
    ARRAY_INT8 = 1016, // _int8 (bigint[])
    ARRAY_TEXT = 1009, // _text (text[])
    ARRAY_VARCHAR = 1015, // _varchar (varchar[])
    ARRAY_FLOAT4 = 1021, // _float4 (float4[])
    ARRAY_FLOAT8 = 1022, // _float8 (float8[])
    ARRAY_BOOL = 1000, // _bool (boolean[])
    ARRAY_JSONB = 3807, // _jsonb (jsonb[])
    ARRAY_NUMERIC = 1231, // _numeric (numeric[])
    ARRAY_UUID = 2951, // _uuid (uuid[])
    ARRAY_TIMESTAMPTZ = 1185, // _timestamptz (timestamptz[])

    _, // Placeholder for unsupported types
};
/// Whether this OID has a decoder. `PgOid` is non-exhaustive, so `@enumFromInt` accepts
/// anything — which is what makes the unknown case silent unless it is asked about
/// explicitly.
pub fn isKnownOid(oid: u32) bool {
    inline for (@typeInfo(PgOid).@"enum".fields) |field| {
        if (field.value == oid) return true;
    }
    return false;
}


pub const NUMERIC_POS: u16 = 0x0000;
pub const NUMERIC_NEG: u16 = 0x4000;
pub const NUMERIC_NAN: u16 = 0xC000;
pub const NUMERIC_PINF: u16 = 0xD000;
pub const NUMERIC_NINF: u16 = 0xF000;

pub const NBASE: u16 = 10000; // Each digit represents 4 decimal digits
