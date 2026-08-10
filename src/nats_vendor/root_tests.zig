// Copyright (c) 2025 g41797
// SPDX-License-Identifier: MIT
//
// Adapted from upstream g41797/nats 0.0.3 for the vendored Zig 0.16 copy.
//
// Excluded relative to upstream:
//   nkeys.zig      — nkey auth was not vendored (this bridge uses user/password)
//   net_tests.zig  — depends on std.net, which Zig 0.16 removed; needs porting
//
// integration_tests.zig is deliberately not aggregated here: it requires a live
// NATS server, so it gets its own build step (`zig build test-nats-integration`).

test {
    _ = @import("protocol.zig");

    _ = @import("parse_tests.zig");
    _ = @import("misc_tests.zig");
    _ = @import("core_tests.zig");
    _ = @import("jetstream_tests.zig");
    _ = @import("subscriber_tests.zig");
    _ = @import("consumer_tests.zig");

    @import("std").testing.refAllDecls(@This());
}
