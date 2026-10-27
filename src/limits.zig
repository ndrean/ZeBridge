//! The identifier limits, in BYTES, in one place.
//!
//! Everything here measures `.len` of a UTF-8 string, never a character count: PostgreSQL's
//! `octet_length`, not `length`. A tenant of 40 two-byte characters is 80 bytes.
//!
//! Three kinds of limit meet in a CDC subject `<prefix>.<tenant>.<table>.<op>[.<suffix>]`:
//!
//!   * PostgreSQL's: a table name is at most 63 bytes (`NAMEDATALEN - 1`); the tenant index
//!     `<table>_zb_ri` that `zebridge_enable` creates leaves 62 for a tenant-scoped table.
//!   * ZeBridge's own: a tenant id is at most `max_tenant_bytes`. It is row data, so the bridge
//!     cannot refuse it at the door of PostgreSQL; the tenant guard trigger does (`${MAX_TENANT_BYTES}`
//!     in init.write.template.sql), `--diagnose` finds what is already there, and the event path
//!     drops what still arrives (counted, by reason: `bridge_dropped_events_total`).
//!   * The client library's: nats.zig refuses to SEND a subject longer than its `max_subject_len`.
//!     The pinned library (before the fork's `zebridge 42`) said 255, which is not a NATS limit:
//!     nats-server 2.15.1 delivers a core subject of 3000 bytes and serves JetStream API subjects
//!     of 550 bytes (NATS_ZIG_NOTES.md, entry 43). Under 255, what bounds a chain read is the
//!     direct get of an object's meta record,
//!     `$JS.API.DIRECT.GET.OBJ_gen-<tenant>.$O.gen-<tenant>.M.<base64(<table>-i<n>-g<n>-full)>`:
//!     38 bytes + twice the tenant + the base64 of the object name, which carries the TABLE and
//!     two counters that grow. So under 255 the tenant limit depends on the table, and a limit
//!     that holds for every table is 46 bytes (`legacy_tenant_bytes`, derived below, 63-byte table
//!     and 10-digit counters). 92 was measured with an 8-byte table and is NOT that limit: a 62-byte
//!     table with a 92-byte tenant never got a chain (walls.py, 2026-10-27).
//!
//! The ring slot holds the subject inline, so its buffer is sized for the longest SUPPORTED
//! combination, not for the typical one: no accepted tenant and table can overflow it. The check in
//! the event path stays as a backstop for what the sizing does not cover (an overlong prefix is
//! refused at boot instead).
const std = @import("std");
const nats = @import("nats");

/// PostgreSQL `NAMEDATALEN - 1`.
pub const max_table_bytes: usize = 63;
/// A tenant-scoped table: `zebridge_enable` names its replica-identity index `<table>_zb_ri`, a
/// `name` of 63 bytes, so a 63-byte table would be given its own name (refused with an instruction).
pub const max_tenant_scoped_table_bytes: usize = max_table_bytes - 1;

/// Base64 (padded) length of `n` bytes: how an object's name appears in its meta subject.
fn base64Bytes(n: usize) usize {
    return (n + 2) / 3 * 4;
}

/// The tenant that fits every table under the library's old 255-byte subject check:
/// `DIRECT.GET` subject = 38 + 2 * tenant + base64(table + "-i" + 10 digits + "-g" + 10 digits + "-full").
pub const legacy_tenant_bytes: usize = (255 - 38 - base64Bytes(max_table_bytes + "-i-g-full".len + 20)) / 2;

/// The tenant id: 128 bytes once the client library accepts the subjects that follow from it
/// (fork patch `zebridge 42`, which exports `max_subject_len`), `legacy_tenant_bytes` before.
pub const max_tenant_bytes: usize = if (@hasDecl(nats, "max_subject_len")) 128 else legacy_tenant_bytes;

/// `insert`, `update`, `delete`.
pub const longest_operation: usize = 6;
/// `transition` (`event_processor.zig`: the suffix of an UPDATE on a table with transition rules).
pub const longest_suffix: usize = "transition".len;

/// The inline subject buffer of a ring slot. `u8` length, so at most 255.
pub const subject_buffer_bytes: usize = 224;

/// Bytes of `<prefix>.<tenant>.<table>.<op>.<suffix>` plus the sentinel, for given lengths.
pub fn cdcSubjectBytes(prefix: usize, tenant: ?usize, table: usize, op: usize, suffix: ?usize) usize {
    return prefix + 1 + (if (tenant) |t| t + 1 else 0) + table + 1 + op + (if (suffix) |s| s + 1 else 0) + 1;
}

/// The longest CDC subject a supported tenant, table, operation and suffix can make.
pub fn longestCdcSubjectBytes(prefix: usize) usize {
    return cdcSubjectBytes(prefix, max_tenant_bytes, max_table_bytes, longest_operation, longest_suffix);
}

/// The longest subject prefix (`cdc` by default) for which every supported combination fits.
pub const max_prefix_bytes: usize = subject_buffer_bytes - longestCdcSubjectBytes(0);

/// Whether the subject of these lengths fits the slot's buffer. The event path's backstop.
pub fn fits(prefix: usize, tenant: ?usize, table: usize, op: usize, suffix: ?usize) bool {
    return cdcSubjectBytes(prefix, tenant, table, op, suffix) <= subject_buffer_bytes;
}

comptime {
    // A change to any limit that breaks the sizing is a compile error, not a dropped row.
    std.debug.assert(subject_buffer_bytes <= std.math.maxInt(u8));
    std.debug.assert(max_prefix_bytes >= "cdc".len);
}

const testing = std.testing;

test "the buffer holds the longest supported subject for the default prefix" {
    try testing.expect(fits("cdc".len, max_tenant_bytes, max_table_bytes, longest_operation, longest_suffix));
    try testing.expect(longestCdcSubjectBytes("cdc".len) <= subject_buffer_bytes);
}

test "a prefix beyond max_prefix_bytes is where the sizing ends" {
    try testing.expect(fits(max_prefix_bytes, max_tenant_bytes, max_table_bytes, longest_operation, longest_suffix));
    try testing.expect(!fits(max_prefix_bytes + 1, max_tenant_bytes, max_table_bytes, longest_operation, longest_suffix));
}

test "the combination that used to lose CDC is inside the supported range" {
    // A 90-byte tenant and a 40-byte table: 128-byte buffer overflowed (143 bytes needed).
    try testing.expect(cdcSubjectBytes(3, 90, 40, 6, null) > 128);
    try testing.expect(fits(3, 90, 40, 6, null));
    try testing.expect(fits(3, 90, 40, 6, longest_suffix));
}

test "the legacy limit is what keeps the longest chain subject under 255" {
    // 38 + 2T + base64(63 + "-i-g-full" + 20 digits) <= 255 for T = legacy_tenant_bytes, and not for one more
    const name_bytes = max_table_bytes + "-i-g-full".len + 20;
    try testing.expect(38 + 2 * legacy_tenant_bytes + base64Bytes(name_bytes) <= 255);
    try testing.expect(38 + 2 * (legacy_tenant_bytes + 1) + base64Bytes(name_bytes) > 255);
    try testing.expectEqual(@as(usize, 46), legacy_tenant_bytes);
}

test "a tenant token is checked in bytes, against the limit" {
    const utils = @import("utils.zig");
    const at: [max_tenant_bytes]u8 = @splat('a');
    const over: [max_tenant_bytes + 1]u8 = @splat('a');
    try testing.expect(utils.isSubjectToken(&at, max_tenant_bytes));
    try testing.expect(!utils.isSubjectToken(&over, max_tenant_bytes));
    // two-byte characters count twice: 47 of them are 94 bytes
    var wide: [94]u8 = undefined;
    for (0..47) |i| @memcpy(wide[2 * i ..][0..2], "é");
    try testing.expectEqual(max_tenant_bytes >= 94, utils.isSubjectToken(&wide, max_tenant_bytes));
    try testing.expect(!utils.isSubjectToken(&wide, 93));
}
