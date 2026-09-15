//! One place that turns the parsed NATS address into what nats.zig needs: the URL to
//! dial and the TLS options (§10fz). The bridge opens several connections (publisher,
//! ingress, generation producer and its workers, fleet monitor); each used to format
//! `nats://host:port` itself, which is how a scheme decision gets lost in one of them.
const std = @import("std");
const nats = @import("nats");
const config = @import("config.zig");

pub fn tlsOptions(ep: config.Nats.Endpoint) ?nats.TlsOptions {
    if (!ep.tls) return null;
    return .{
        .ca_file = ep.tls_ca,
        .cert_file = ep.tls_cert,
        .key_file = ep.tls_key,
        .server_name = ep.tls_server_name,
    };
}

pub fn dialUrl(a: std.mem.Allocator, ep: config.Nats.Endpoint) std.mem.Allocator.Error![]u8 {
    return ep.dialUrl(a);
}
