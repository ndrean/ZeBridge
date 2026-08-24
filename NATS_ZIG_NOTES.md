# Local changes to nats.zig

Fixes made while migrating ZeBridge onto this library. Each entry: what broke, how it
showed up, what changed. Upstream candidates unless noted.

---

## 1. Use-after-free in `pullSubscribe` when the stream is derived from the subject

**How it appeared**

A pull subscription created without an explicit `.stream`, then fetched:

```zig
const sub = try js.pullSubscribe("zb.spike.one", "zb_spike_worker", .{
    .config = .{ .ack_policy = .explicit, .max_deliver = 3, .filter_subject = "zb.spike.one" },
});
var batch = try sub.fetch(1, .{ .duration = .{ .raw = .fromSeconds(3), .clock = .awake } });
```

```
error: InvalidSubject
  src/validation.zig:144  validateSubject
  src/connection.zig:661  publishRequest
  src/jetstream.zig:468   fetch
```

Misleading symptom: the subject was not malformed, it was **freed**. `validateSubject`
was reading whatever the allocator had put back in that memory.

**Cause**

`pullSubscribe` resolves the stream name, and frees it on return when it had to look it
up:

```zig
const stream_name = if (options.stream) |s| s
    else if (subject) |s| try self.lookupStreamBySubject(s)   // heap-allocated
    else return error.StreamOrSubjectRequired;

defer if (options.stream == null and subject != null) self.nc.allocator.free(stream_name);
```

…but then stores that same pointer in the `PullSubscription`, which outlives the call:

```zig
pull_subscription.* = PullSubscription{ .stream_name = stream_name, ... };
```

`fetch` later builds `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>` from it. Passing
`.stream` explicitly avoids the path entirely, which is why it is easy to miss.

**Change**

`src/jetstream.zig`:

* `pullSubscribe` — `defer free` → `errdefer free`. Ownership moves to the subscription
  once it is constructed; the `errdefer` still covers the failure paths before that.
* `PullSubscription` — new `owns_stream_name: bool`, set from
  `options.stream == null and subject != null`. Needed because by `deinit` time the
  caller's `options` are long gone, so the struct has to remember whether the memory is
  its own.
* `PullSubscription.deinit` — frees `stream_name` when it owns it.

⚠️ **`subscribe` (push) has the identical line and it is correct there** — that function
only uses `stream_name` within its own body. I patched it first by matching on the text
and had to revert: it would have leaked on every push subscription. The two lines look the
same; only the lifetime differs.

**Verified**

```bash
cd spike && zig build && ./zig-out/bin/spike     # 12/12, on the path that used to crash
cd nats.zig && zig build test                    # 104 + 167 tests pass
```

The spike deliberately omits `.stream` so it takes the lookup path. A caller that passes
`.stream` (as ZeBridge normally would, since topology.json names it) never hits this,
which is why it survived 249 commits.


---

## 2. `UNSUB` on a closed connection logged at error level

**How it appeared**

Every clean shutdown of the bridge, once per subscription:

```
info(nats): Closing connection
info(nats): Disconnected
error(nats): Failed to send UNSUB for sid 1: error.Closed
error(nats): Failed to send UNSUB for sid 1: error.Closed
error(nats): Failed to send UNSUB for sid 1: error.Closed
```

Note the ordering: the errors arrive *after* the connection is reported closed, which is
the clue. Three of them because the bridge has three subscribers (publisher, snapshot
listener, mutation listener).

**Cause**

`Connection.unsubscribe` (connection.zig ~:914) treats every failure of
`unsubscribeInternal` as an error worth reporting. But sending `UNSUB` on a socket that is
already closed cannot succeed *and does not need to* — the server drops all subscriptions
for a connection when it goes away. It is an expected step in an ordinary shutdown, not a
fault.

Harmless in itself; the cost is that an operator learns to ignore `error(nats)`.

**Change**

`src/connection.zig` — `error.Closed` now logs at `debug` with the reason, `error.Canceled`
keeps its `recancel()` path, everything else still logs at error level.

**Verified**

```bash
kill -TERM <bridge>     # 0 occurrences, was 3
cd nats.zig && zig build test    # 104 + 167 tests pass
```

---

## 3. A wildcard inbox makes every pull request spawn another

**How it appeared**

An idle stream — nothing published, nothing to deliver — with two pull consumers, each
looping on `fetch(1, 500ms)`. Measured against nats-server 2.14.4:

| | |
| --- | --- |
| pull requests issued | **20.7/s** on one consumer, 3.6/s on the other |
| period between requests | **48 ms**, against a requested `expires` of **500 ms** |
| `num_waiting` on the consumer | **10** and **2** — parked requests, not one in flight |
| NATS traffic, whole server, idle | 17.3 msg/s in *and* out |
| fetch ids reached | 5,911 in 286 s, monotonic — genuinely new requests, not retries |

The server was answering each abandoned request with `408 Request Timeout` at its expiry, so
the debug log filled with 6,658 status frames in under five minutes on a stream with no data.

**Why**

`PullSubscription.fetch` mints a unique reply subject per request:

```zig
const reply_subject = try std.fmt.allocPrint(..., "{s}{d}", .{ self.inbox_prefix, fetch_id });
```

…then reads replies from `self.inbox_subscription`, which is a **wildcard** (`<prefix>.*`).
So it also receives replies addressed to *earlier* fetches. A fetch that returns before its
own `expires` leaves its request parked server-side until the server times it out, and that
late `408` then lands here while a **newer** fetch is waiting. The newer fetch treats it as
its own timeout, returns immediately, the caller re-fetches at once — and that request is
abandoned in turn. Self-sustaining, settling at `expires ÷ period` parked requests, which is
exactly the 10 and 2 observed against a 500 ms expiry.

Nothing is lost, because the wildcard also picks up the real replies. The costs are the
request storm, the CPU, and `num_waiting` climbing toward `max_waiting` (512).

**The fix** (`nats.zig-jetstream-stale-408.patch`, hunk at `@@ -483`)

Discard a frame whose subject is not this fetch's reply subject, and keep waiting:

```zig
if (raw_msg.status_code > 0 and !std.mem.eql(u8, raw_msg.subject, reply_subject)) {
    raw_msg.deinit();
    continue;
}
```

⚠️ **Status frames only — this is the part that bites.** The first attempt matched *every*
frame against `reply_subject` and broke `jetstream_pull_test` "basic fetch" (expected 2
messages, got 0). JetStream delivers a **data** message with its *original* subject
(`test.foo`), routed to the inbox by subscription id; only status frames are addressed to the
inbox itself (`HMSG _INBOX.<prefix>.<fetch_id>`). Matching data messages against the inbox
therefore drops every real message. The existing comment — *"the timestamp in the ACK subject
ensures messages belong to this fetch request"* — reasons about data messages and does not
cover status frames.

After the fix, same idle stream: **3.8 msg/s** server-wide (from 17.3), and `num_waiting`
sits at **1** on both consumers instead of 10 and 2.

`zig build test-unit` 104/104 and `zig build test-e2e` 167/167 pass with it applied.

**Reproducing without ZeBridge**

Create a pull consumer on an empty stream, loop `fetch(1, 500ms)`, and watch
`nats consumer info <stream> <consumer>`. `num_waiting` should stay at 1; it climbs to
`expires ÷ actual_period` instead. A caller cannot detect this from the API: `fetch` reports
408 in `MessageBatch.err` rather than returning an error, so `catch { continue; }` never fires
and an empty `messages` slice looks like a normal idle result.

---

## 4. Submodule bumped to upstream `d4cd40d` (2026-08-24)

`b3684bd` ("Retry JetStream publishes on no responders", #148) → `d4cd40d`
("Type-check the public surface", #157) — 11 upstream commits. None of them contain
fixes 1–3 above, so both local patches were re-applied; both applied cleanly with no
re-fit (`git apply` straight from `nats.zig-connection.patch` and
`nats.zig-jetstream-stale-408.patch`).

What the bump brings, relevant to ZeBridge:

* **#153/#154/#156** — lock-order inversion and deinit races around `subs_mutex` in
  `connection.zig`, the same shutdown region fix 2 touches.
* **TLS** (#150 series, incl. mutual TLS) — the client can now speak `tls://`. Does not
  change the v1.0 colocation decision, but the "no TLS client exists" premise behind it
  is no longer true.
* **#157** — type-checks the public surface; three API functions never compiled before.

Verified after the bump: `zig build` and `zig build test` clean (Debug and ReleaseFast),
bridge boots against the Docker stack, `keys.py` passes 7/7 (exercises the pull-fetch
loop fix 3 guards), and the `MUTATIONS` durable shows `num_waiting: 0` after the run —
no parked-pull storm.

**TLS verified (2026-08-24).** The library's own e2e TLS suite passes 8/8 against real
`nats-server` containers (`TEST_FILTER="tls e2e" zig build test-e2e`): CA-verified
connect, verification failure without the CA, insecure-skip-verify, handshake-first,
mutual TLS with `verify_and_map` as the authentication, rejection without/with an
unmapped client cert, and reconnect over TLS. On top of that, a standalone probe ran the
bridge's workload shape — `addStream`, acked `js.publish`, durable `pullSubscribe` +
`fetch` + ack — against a host `nats-server` with only a `tls{}` listener
(JetStream on, CA-verified, no insecure flags): all green. That probe is now permanent:
`scripts/scenarios/tls.py` provisions the server, builds the probe against the
submodule, and asserts both the verified round trip and that a connect *without* the CA
is refused (`CertificateIssuerNotFound`). So `tls://` is real for
both core NATS and JetStream, which reopens the door the colocation constraint closed —
a bridge talking to a remote NATS (e.g. a per-tenant leaf) over TLS is now a client
capability, not a wish.
