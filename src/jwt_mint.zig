//! Mint NATS user JWTs — the bridge as online signer (NOTES: the JWT mint flow).
//!
//! A user JWT under a SCOPED signing key carries NO permissions: just the name,
//! the user's public key, the tenant tag and an expiry — `pub:{} sub:{}` are
//! REQUIRED-EMPTY (the server refuses anything else from a scoped issuer). The
//! grants live in the account JWT's role template, preloaded in the server conf;
//! the signature by THIS key is what selects them. So compromise of this seed can
//! only ever mint more clients — never widen a permission.
//!
//! Wire shape: `base64url(header).base64url(claims)` signed ed25519, signature
//! base64url-appended. `jti` is the base32(sha256) of the claims serialized with
//! an empty jti — the same convention the Go jwt library uses; the server treats
//! it as an opaque ID, so self-consistency is what matters.

const std = @import("std");
const nats = @import("nats");

const b64 = std.base64.url_safe_no_pad.Encoder;

/// RFC 4648 base32, upper-case, no padding — what nkeys/jti use. std has none.
fn base32Encode(out: []u8, input: []const u8) []const u8 {
    const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    var acc: u16 = 0;
    var bits: u4 = 0;
    var n: usize = 0;
    for (input) |byte| {
        acc = (acc << 8) | byte;
        bits += 8;
        while (bits >= 5) {
            bits -= 5;
            out[n] = alphabet[@as(usize, (acc >> bits) & 0x1f)];
            n += 1;
        }
    }
    if (bits > 0) {
        out[n] = alphabet[@as(usize, (acc << (5 - bits)) & 0x1f)];
        n += 1;
    }
    return out[0..n];
}

pub const MintError = error{ BadSeed, OutOfMemory };

/// Returns the signed JWT (caller frees). `signing_seed_text` is the scoped
/// signing key's seed ("SA..."), `account_pub` the account's public key ("A..."),
/// `user_pub` the client-generated user public key ("U...") — the seed side of
/// that pair never reaches this process.
pub fn mint(
    allocator: std.mem.Allocator,
    signing_seed_text: []const u8,
    account_pub: []const u8,
    principal: []const u8,
    tenant: []const u8,
    user_pub: []const u8,
    ttl_seconds: i64,
    now_unix: i64,
) MintError![]u8 {
    var kp = nats.nkeys.SeedKeyPair.fromSeed(signing_seed_text) catch return error.BadSeed;
    defer kp.wipe();
    var iss_buf: [nats.nkeys.public_key_text_len]u8 = undefined;
    const iss = kp.publicKeyText(&iss_buf);

    const iat = now_unix;
    const exp = now_unix + ttl_seconds;

    // Claims twice: once with jti empty (hashed), once with the real jti. Field
    // order only matters for self-consistency — the server never recomputes it.
    const claims_fmt =
        "{{\"jti\":\"{s}\",\"iat\":{d},\"exp\":{d},\"iss\":\"{s}\",\"name\":\"{s}\",\"sub\":\"{s}\"," ++
        "\"nats\":{{\"pub\":{{}},\"sub\":{{}}," ++
        "\"issuer_account\":\"{s}\",\"tags\":[\"tenant:{s}\"],\"type\":\"user\",\"version\":2}}}}";

    const hashed = try std.fmt.allocPrint(allocator, claims_fmt, .{ "", iat, exp, iss, principal, user_pub, account_pub, tenant });
    defer allocator.free(hashed);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(hashed, &digest, .{});
    var jti_buf: [56]u8 = undefined;
    const jti = base32Encode(&jti_buf, &digest);

    const claims = try std.fmt.allocPrint(allocator, claims_fmt, .{ jti, iat, exp, iss, principal, user_pub, account_pub, tenant });
    defer allocator.free(claims);

    const header = "{\"typ\":\"JWT\",\"alg\":\"ed25519-nkey\"}";

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    const h64_len = b64.calcSize(header.len);
    const c64_len = b64.calcSize(claims.len);
    try out.ensureTotalCapacity(allocator, h64_len + 1 + c64_len + 1 + b64.calcSize(64));
    out.items.len = h64_len;
    _ = b64.encode(out.items[0..h64_len], header);
    try out.append(allocator, '.');
    const c_start = out.items.len;
    out.items.len += c64_len;
    _ = b64.encode(out.items[c_start..], claims);

    // The signature covers exactly the "header.claims" ASCII.
    const sig = kp.sign(out.items) catch return error.BadSeed;
    try out.append(allocator, '.');
    const s_start = out.items.len;
    out.items.len += b64.calcSize(64);
    _ = b64.encode(out.items[s_start..], &sig);

    return out.toOwnedSlice(allocator);
}
