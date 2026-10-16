//! Every HTTP response from the bridge's servers (http_server.zig, demo_invites.zig): one
//! request per connection, so `keep_alive = false`, always.
//!
//! ⚠️ Not only a policy. With keep-alive, std.http.Server prepares the connection for a
//! next request by discarding the body, and asserts the request declared one
//! (Content-Length or chunked) when its method may carry one. A `POST` with neither (curl
//! -X POST sends exactly that) then panics a Debug build, and a ReleaseFast one runs past
//! the compiled-out assert: undefined behaviour from one request. Found with a Debug build
//! of --demo-invites under load (2026-10-16). Closing skips that path: the body is never read.
const std = @import("std");

pub fn respond(req: *std.http.Server.Request, content: []const u8, options: std.http.Server.Request.RespondOptions) !void {
    var o = options;
    o.keep_alive = false;
    return req.respond(content, o);
}
