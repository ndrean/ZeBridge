//! `bridge --gen-nkey`: mint the nkey pair a DBA installs between NATS and the bridge.
//!
//! The crypto is std's, the FORMAT is nats.zig's. `Ed25519.KeyPair.generate` draws 32
//! random bytes — that seed IS the private key (Ed25519 derives the public key from
//! it), and `nkeys.encodeSeed` renders those same bytes in NATS's text encoding:
//! `base32(prefix('S' + 'U') || seed || crc16)` → `SU…`. Nothing is generated twice.
//!
//! ⚠️ The two lines go to STDOUT and the warning to STDERR, so the command pipes:
//!
//!     bridge --gen-nkey >> .env.bridge      # the KEY=value lines land in the file,
//!                                           # the warning stays on the terminal
const std = @import("std");
const nats = @import("nats");

pub fn genNkey(io: std.Io) !void {
    const kp = std.crypto.sign.Ed25519.KeyPair.generate(io);
    var seed_buf: [nats.nkeys.seed_text_len]u8 = undefined; // "SU" + base32(2+32+2 bytes)
    const seed: []const u8 = nats.nkeys.encodeSeed(.user, &kp.secret_key.seed(), &seed_buf);

    // Round-tripped rather than read off `kp` directly: this decodes the very text
    // about to be printed (prefix, checksum and all) and derives the public key from
    // it, so a seed that cannot be read back never leaves this function.
    var pk = try nats.nkeys.SeedKeyPair.fromSeed(seed);
    defer pk.wipe();
    var pub_buf: [nats.nkeys.public_key_text_len]u8 = undefined;
    const public = pk.publicKeyText(&pub_buf);

    // ⚠️ `std.Io.Writer.fixed(&buffer)` would format into `buffer` and stop there — a
    // fixed writer's sink IS the array, it has no file descriptor to reach. A
    // File.Writer owns the same kind of buffer but drains it to the fd on `flush`,
    // which is the call that actually prints. Streaming, not positional: stdout is a
    // pipe as often as a file, and a pwrite on a pipe is a syscall that must fail first.
    var out_buf: [256]u8 = undefined;
    var out = std.Io.File.stdout().writerStreaming(io, &out_buf);
    try out.interface.print("NATS_BRIDGE_NKEY_PUB={s}\nNATS_BRIDGE_NKEY_SEED={s}\n", .{ public, seed });
    try out.interface.flush();

    try std.Io.File.stderr().writeStreamingAll(io, "\n⚠️  the seed IS the credential: store it like a password, it is shown once\n");
}
