# Local changes to nats.zig

Fixes made while migrating my app onto this library.
Each entry: what broke, how it showed up, what changed. Upstream candidates unless noted.

The submodule stays at the upstream commit the parent records (`d4cd40d`); the changes
live in its working tree and, since 2026-09-23, as an **ordered series** of seventeen
patches here, `01-…` to `17-…`, each the exact diff between two consecutive states. They
apply in numeric order and only in that order. Two scripts keep it honest:

    nats.zig-patch/check-series.sh          upstream + series == working tree, or say where not
    nats.zig-patch/new-patch.sh <topic>     cut the next patch as diff(upstream + series, working tree)

Never cut a patch from `git -C nats.zig diff` with hunks filtered by hand — §17 is what
that produced. Entry numbers below are chronological and are not the series numbers;
the table in §17 maps one onto the other.

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

Misleading symptom: the subject was not malformed, it was **freed**.
`validateSubject` was reading whatever the allocator had put back in that memory.

**Cause**

`pullSubscribe` resolves the stream name, and frees it on return when it had to look it up:

```zig
const stream_name = if (options.stream) |s| s
    else if (subject) |s| try self.lookupStreamBySubject(s)   // heap-allocated
    else return error.StreamOrSubjectRequired;

defer if (options.stream == null and subject != null) self.nc.allocator.free(stream_name);
```

but then stores that same pointer in the `PullSubscription`, which outlives the call:

```zig
pull_subscription.* = PullSubscription{ .stream_name = stream_name, ... };
```

`fetch` later builds `$JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>` from it.
Passing `.stream` explicitly avoids the path entirely, which is why it is easy to miss.

**Change**

`src/jetstream.zig`:

* `pullSubscribe` — `defer free` → `errdefer free`.
  Ownership moves to the subscription once it is constructed; the `errdefer` still covers the failure paths before that.
* `PullSubscription` — new `owns_stream_name: bool`, set from `options.stream == null and subject != null`. Needed because by `deinit` time the caller's `options` are long gone, so the struct has to remember whether the memory is its own.
* `PullSubscription.deinit` — frees `stream_name` when it owns it.

⚠️ **`subscribe` (push) has the identical line and it is correct there** — that function only uses `stream_name` within its own body. I patched it first by matching on the text and had to revert: it would have leaked on every push subscription. The two lines look the same; only the lifetime differs.

**Verified**

```bash
cd spike && zig build && ./zig-out/bin/spike     # 12/12, on the path that used to crash
cd nats.zig && zig build test                    # 104 + 167 tests pass
```

This survived 249 commits.


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

Note the ordering: the errors arrive *after* the connection is reported closed, which isthe clue.
Three of them because my app has three subscribers.

**Cause**

`Connection.unsubscribe` (connection.zig ~:914) treats every failure of `unsubscribeInternal` as an error worth reporting.
But sending `UNSUB` on a socket that is already closed cannot succeed *and does not need to* — the server drops all subscriptions for a connection when it goes away.
It is an expected step in an ordinary shutdown, not a fault.

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

## 5. The deliberate close logged like an incident

**How it appeared**

Every generation-producer tick — the producer opens a fresh connection per tick and
closes it on the way out — the bridge log gained:

```
info(nats): Closing connection
info(nats): Disconnected
```

Reported from review: an operator saw the pair recurring, cross-checked
`nats_reconnects=0` on /status, and reasonably asked whether NATS was reconnecting
silently or the counter was broken. Neither — the counter was right and nothing was
wrong, which is exactly the problem: a routine, caller-initiated close should not read
as a connection event worth investigating.

**Cause**

`Connection.close()` logs "Closing connection" at info, and the connection loop's exit
logs "Disconnected" at info whenever it ends without an error — including after a
deliberate `close()`. Both lines are about an event the CALLER caused.

**Change**

`src/connection.zig` — both lines drop to `debug`. Real failures keep their voice:
an unexpected drop logs `Connection failed: {err}` (still info, inside the same exit
path but only when an error is present) and the reconnect machinery reports at warn.
A server-initiated clean EOF also lands on the quieted line, but the reconnect path
announces itself immediately after, so the signal is not lost.

`nats.zig-quiet-deliberate-close.patch` (hunks 1 and 3 of the connection.zig diff; the
middle hunk is entry 2's).

**Verified**

```bash
cd nats.zig && zig build test        # 132 + 183 tests pass
# live bridge, 65s at LOG_LEVEL=info: 0 occurrences of the pair (was 2 per cadence tick)
```

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

## 6. The terminal auth verdict, retrievable (2026-09-04)
`Connection.final_auth_error` + `lastAuthError()`. When the server states an auth
verdict — `-ERR 'User Authentication Expired'`, revoked, or violation — the reader and
the two-strikes reconnect path both record it, so a caller that later finds the
connection closed can NAME the cause instead of reporting a bare `ConnectionClosed`.
libzb's C ABI consults it on any failed poll (ZeBridge NOTES §10cj). Patch:
nats.zig-auth-verdict.patch. 132+183 tests pass.

---

## 7. Three standing patches that never got their entry (ledgered 2026-09-04)

All three shipped with libzb's live-tailing work (ZeBridge NOTES §10bh) and were
referenced from there but never written up HERE — found when a review asked
whether a submodule diff was recorded. Nothing new in the diff; this closes the
bookkeeping gap.

**`nats.zig-fetch-early-return-503.patch`** — `PullSubscription.fetch` parked
terminal status frames (503 and friends) in `MessageBatch.err` and returned an
EMPTY batch, so a caller's `catch` never fired and "the consumer is gone" looked
identical to "nothing to read". A terminal status is now a returned error; the
dead `batch.err` arm in libzb's drain loop became live the same day (see the
comment at libzb client.zig's bounded drain).

**`nats.zig-consumer-inactive-threshold.patch`** — `ConsumerConfig` gains
`inactive_threshold`: the SERVER reaps an idle consumer after the given
nanoseconds. libzb names its tail consumers and lets the server own their
lifetime (30 s past any real fetch gap) instead of deleting them client-side —
a client that dies uncleanly leaves nothing behind.

**`nats.zig-shared-pull-inbox.patch`** — `PullInbox`: ONE wait over many pull
subscriptions — one pull per tail into a shared inbox, first message from any
of them ends the wait. Replaced libzb's 250 ms round-robin whose cost was a
FIXED idle slice per stream (measured 265 ± 1 ms added latency on every write).
Carries `owns_inbox` so a subscription on a shared inbox does not free it, and
the addendum hunk: the all-consumers-gone path freed `gone` twice (errdefer +
explicit) — a macOS SIGTRAP found by stream_wipe.py, 2026-09-03.

---

## 8. Double release of the meta message on a deleted object (2026-09-06)

**How it appeared**

libzb's host (a Python process over the C ABI) aborted at `zb_client_close` after a
seed that had failed on a chain object the producer had just deleted:

```
malloc: *** error for object 0x...: pointer being freed was not allocated
```

Found with `lldb -o "breakpoint set -n malloc_error_break"`: the second free came from
`ObjectStore.get`, on the meta message of an object whose info said `deleted: true`.

**Cause**

`get()` and `infoIncludingDeleted()` fetch the object's meta message, and on the
deleted path release it explicitly before returning `error.ObjectNotFound` — while an
`errdefer meta_msg.deinit()` above releases it again on that same error return.
Reachable by any caller that reads an object between the producer's delete and the
manifest swap, which the generation chain does on every prune.

**Change** (`nats.zig-objstore-double-release.patch`)

`src/jetstream_objstore.zig`: an owned flag on the meta message —
`var meta_msg_owned = true; errdefer if (meta_msg_owned) meta_msg.deinit();` — cleared
right after the explicit release, in both functions.

`src/root.zig`: exports `KVWatcher` and `WatchOptions`, which libzb's live schema
watch needs (ZeBridge NOTES §10de); the types existed, only the re-export was missing.

**Verified**

```bash
cd libzb && zig build test                    # unit tests
libzb/python/migrate_reseed.py, migrate_rekey.py   # the seed-after-delete path, PASS
```

Upstream checked 2026-09-06 (`git fetch`, `d4cd40d` still the tip): none of entries 1–8
are upstream. A PR bundling 1, 3, and 8 would carry the least ZeBridge-specific
context; 6 and 7 are API additions and need a discussion first.
## 9. The routine connect logged twice, the consumer add logged like a decision (2026-09-07)

**How it appeared**

Every fleet-monitor tick — the monitor opens a fresh connection per pass, like the
generation producer, and lists the `live` bucket's keys — the bridge log gained:

```
info(nats): Connected successfully to nats://127.0.0.1:4222
info(nats): Connected successfully
info(nats): adding consumer
```

Reported from the web-consumer session: an operator saw the trio recur every minute,
next to a `JetStream error … 10058` that turned out to be the bridge's own
create-then-open of the bucket (fixed on the bridge side, NOTES §10dr), and asked
whether the bridge was reconnecting. It was not — it was connecting, on purpose, on
schedule — and entry 5's reasoning applies unchanged: a routine, caller-initiated event
should not read as one worth investigating.

**Cause**

`connect()` announces success twice at info: once with the URL in the per-server
connect path, once more in the wait loop that returns to the caller. `addConsumer`
logs "adding consumer" at info before every CONSUMER.CREATE, which a KV `keys()` issues
each time it is called.

**Change**

`src/connection.zig` — both "Connected successfully" lines drop to `debug`.
`src/jetstream.zig` — "adding consumer" drops to `debug`. Real events keep their
voice: a drop and the re-connect after it are reported by the reconnect machinery at
warn, and a JetStream API refusal stays at err (the caller gets the typed error too).

`nats.zig-quiet-routine-connect.patch`.

**Verified**

```bash
cd nats.zig && zig build test
# bridge at LOG_LEVEL=info: the trio gone from the fleet tick; a forced drop still logs the reconnect
```

---
## 10. Every JetStream API refusal logged as an incident (2026-09-08)

**How it appeared**

Two lines at error level on every libzb open and on every fleet-monitor tick:

```
error(nats): JetStream error: code=404 err_code=10014 description=consumer not found
error(nats): JetStream error: code=400 err_code=10058 description=stream name already in use with a different configuration
```

The first is `pullSubscribe`'s look-before-create of the client's consumer, the
second was the bridge's create-then-open of a bucket (fixed on the bridge side,
NOTES §10dr). Both callers handle the typed error and go on; the log said otherwise.

**Cause**

`jetstream.zig`'s API response handler logs `JetStream error: …` at error level for
every error response before mapping it to a typed error. The library cannot know
whether the caller treats the answer as a failure, so the level is wrong by
construction: a 404 on a probe is an answer.

**Change**

`src/jetstream.zig` — the line drops to `debug`. The typed error still reaches the
caller, which reports at the level it means (the bridge's `ensureStream` logs its own
"not reachable" at error, for instance).

`nats.zig-jetstream-error-level.patch`.

**Verified**

```bash
cd nats.zig && zig build test        # unit steps; the integration suite needs its services
# libzb open + fleet tick at LOG_LEVEL=info: both lines gone
```

---
## 11. `PullSubscription.nextDelivered` — what a parked pull still delivers (2026-09-08)

**How it appeared**

A clean stop of ZeBridge's mutation listener left one or two writes "in flight": the
next instance found them pending on the durable consumer and waited an ack wait for
their redelivery (ZeBridge NOTES §10eb). The batch in hand had been committed and
acked; what stayed open was the last pull request.

**Cause**

`fetch` returns as soon as it has something (entry 3's early return, nats.go's legacy
contract), and the server keeps the request parked until its `expires`. Messages
delivered into that window land on the wildcard inbox, which the NEXT `fetch` reads —
fine while fetches keep coming. At shutdown nobody reads it, so those messages are
delivered and unacked. Pulling "until empty" does not close the window either: under
a steady publisher every pull returns something and parks in turn.

**Change**

`src/jetstream.zig` — `PullSubscription.nextDelivered(timeout)`: a data message already
delivered to the consumer's inbox, without a new request; status frames skipped; null
at the deadline. A caller stopping reads the inbox until it has been silent for longer
than the parked request can live, then leaves with nothing in flight.

`nats.zig-pull-next-delivered.patch`.

**Verified**

```bash
# bridge stopped under a 15 writes/s publisher: "N write(s) the parked pull still
# delivered, judged before exit"; the next boot: nothing in flight.
```

---

## 12. Which TLS versions the ClientHello offers is now the caller's choice (2026-09-17)

**How it appeared**

A question, not a fault: "what would it take to move the bridge's NATS hop to TLS 1.3,
and would the shorter handshake help?" Measured before touching anything, against a
throwaway `nats-server` with a `tls{}` listener and the server's own `/connz`:

```json
{"name": "nats.zig", "lang": "zig", "tls_version": "1.3", "tls_cipher_suite": "TLS_AES_128_GCM_SHA256"}
```

It was TLS 1.3 already. `connection.zig` builds `tls.nonblock.Client.init(.{ ... })`
without naming `cipher_suites`, so tls.zig's default applies — `cipher_suites.all`:
the 1.3 suites first, then 1.2's recommended ones, then 1.2's CBC ones — and a Go
`nats-server` picks the highest both sides speak. The handshake saving is per
*connection*, and the bridge holds one for the life of the process; steady-state cost
is record encryption, which 1.3 does not change. No performance work to do.

What the library could not do was say *"1.3 only"*: `TlsOptions` had no field for the
suites, so the ClientHello always carried the 1.2 fallback, CBC suites included, and a
downgrade could always be offered. Hygiene, not speed — but real, and one field away.

**Change** (`nats.zig-tls13-ciphers.patch`, 4 hunks)

`src/connection.zig` — `pub const CipherSuites = enum { default, secure, tls13 }` and a
`cipher_suites: CipherSuites = .default` field on `TlsOptions`, mapped in the
handshake to `tls.config.cipher_suites.{all, secure, tls13}`. An enum rather than
tls.zig's own `[]const CipherSuite`: `TlsOptions` is public surface and is compiled
with `use_tls` off too, where `tls` is `tls_stub.zig` — a type from the dependency
would leak into every caller's build. `.default` maps to exactly the slice tls.zig
used when nothing was said, so nothing changes unless asked.

`src/tls_stub.zig` — the mirror of `config.CipherSuite`, `config.cipher_suites` and
the `Client.cipher_suites` field, nine lines, so the stub still type-checks.

ZeBridge does not set it yet: the colocated `nats-server` speaks 1.3, but pinning the
bridge's hop is a deployment decision (an old TLS-terminating proxy would refuse), not
a library one. When it is taken, it is `.tls = .{ .cipher_suites = .tls13 }`.

**Verified**

```bash
cd nats.zig && zig build test-unit          # 132/132, 20 consecutive runs on a quiet machine
# probe client with .cipher_suites = .tls13 against a tls{} nats-server, read on /connz:
#   tls_version "1.3", tls_cipher_suite "TLS_AES_128_GCM_SHA256"
```

One caution recorded rather than hidden: over 30 runs with the patch and 30 without,
`tls: server key updates are handled and answered` (a 5 s timing test) failed once —
with the patch, on a run that shared the machine with the scenario battery. The
patch cannot reach that test (`.default` is the same slice), so it is filed as the
timing flake it looks like, and watched.

Not in this change, and worth knowing before anyone asks for resumption: tls.zig
supports TLS 1.3 session tickets (`Options.session_resumption`, reachable from the
nonblock path), nats.zig passes none — and in the probe the server issued no ticket,
so check `nats-server` before building on it. Go's TLS server implements no 0-RTT.

---

## 13. The inbox prefix is now the caller's choice (2026-09-19)

**How it appeared**

A read boundary that could not be drawn. JetStream does not deliver a pulled message, a
KV answer or an object chunk on the subject the reader filtered on — it delivers to the
reader's **inbox**, and the subject ACL is never consulted for it. Every NATS client
generates inboxes under one shared `_INBOX.`, so every principal on an account listens in
the same space. Measured again on the dev stack while writing this, with a plain client
credential subscribed to `_INBOX.>`:

```
_INBOX.8YR56W4T5TBBWKEN1IK4C4.147640x0      # another principal's pull deliveries
_INBOX.U8XIEDIS07ZC1BCLU2KT6V.147632x0      # and another's
_INBOX.M7BDIF0FBG61D08SDGXW1N.31298
```

The fix is one grant per principal, `_INBOX.<name>.>`. The library could not be a client
of it: `_INBOX.` was a constant in `inbox.zig` and again in `response_manager.zig`, where
it also sized an inline buffer at compile time.

Not an opinion, an omission: nats.go has `CustomInboxPrefix` (validated: non-empty, no
wildcard, no trailing dot), nats.js has `inboxPrefix` — its own documentation says
"useful for clients with limited subject permissions" — and nats-py has `inbox_prefix`.
nats.zig was the outlier.

**Change** (`nats.zig-inbox-prefix.patch`, 15 hunks, 6 files)

`src/inbox.zig` — `default_prefix` ("_INBOX", no dot), `max_prefix_len` (64),
`validatePrefix` with nats.go's rules plus the length bound, and
`newInboxWithPrefix(allocator, prefix)`. `newInbox(allocator)` keeps its signature and
its output.

`src/connection.zig` — `ConnectionOptions.inbox_prefix`, borrowed like `name` and `user`
rather than duplicated; validated once in `connect()`, so a bad prefix is an error at the
API boundary instead of a silent permissions violation on the first reply; and
`Connection.newInbox(allocator)`, the nats.go shape, which everything inside the library
now calls.

`src/response_manager.zig` — the reply prefix is built from the connection's prefix. The
inline buffer stays inline: its size comes from `max_prefix_len` instead of the literal,
so a custom prefix is bounded rather than allocated (the default uses 30 of those bytes).
The `bufPrint` that could not fail before now returns `error.InvalidInboxPrefix` rather
than `unreachable`, for a manager built by hand.

`src/jetstream.zig`, `src/jetstream_kv.zig`, `src/jetstream_objstore.zig` — the four
generated consumer delivery subjects, the two `PullInbox` bases, the KV watcher and the
two object-store reads go through `Connection.newInbox`. That is what makes the option
cover more than request/reply.

**Verified**

```bash
cd nats.zig && zig build test-unit         # 134/134 (2 new), 17 consecutive runs
git -C nats.zig apply -R … && apply …      # the patch reverses and re-applies cleanly
```

Live, against the dev stack, with the `requestor` example temporarily pointed at it (the
file was restored afterwards):

| prefix | reply subject on the wire |
|---|---|
| default | `_INBOX.O2EZJ87992JNROFX1DMEA2.0` |
| `_INBOX.zbprobe` | `_INBOX.zbprobe.36N9KD6RM95JQR47OKXJDB.0` |

Both replies arrived. ZeBridge adopted it the same day: every client sets
`_INBOX.<principal>` and the account's client and responder templates grant
`_INBOX.{{name()}}.>` instead of `_INBOX.>` (NOTES §10hm).

One run of the 134 failed immediately after the edit, before any of the 17 that followed,
and the failing test was not captured. Recorded rather than hidden; the suite has a known
timing test that flaked once under load in entry 12.

---

## 14. An object store can expire its objects (2026-09-20)

**How it appeared**

An answer too large for one NATS message goes to an object store and the asker reads it
(ZeBridge §10hq). Such a store holds TRANSIENT objects: read once, then rubbish. The
backing stream already has `max_age` — `ObjectStoreConfig` had no way to set it, so
`createStore` sent a stream with no age at all and the only way to bound the bucket was
`max_bytes` or a sweeper of one's own.

**Change** (`nats.zig-objstore-max-age.patch`, 2 hunks, 1 file)

`src/jetstream_objstore.zig` — `max_age_ns: u64 = 0` on `ObjectStoreConfig`, passed to
the stream config `createStore` already builds. Zero keeps an object for ever, which is
what every store created before this got, so nothing changes unless asked. nats.go
spells the same thing `TTL` on its ObjectStoreConfig.

**Verified**

```bash
cd nats.zig && zig build test-unit          # 134/134
git -C nats.zig apply -R … && apply …       # the patch reverses and re-applies cleanly
```

Live: ZeBridge's responders create `res-<tenant>` with 600 s and the server reports it —
`OBJ_res-_default max_age 600 s` in `scripts/scenarios/serve.py`, alongside a 1.24 MB
answer that travelled as an object and arrived whole in both client libraries.

---

## 15. The JetStream domain is now the caller's choice (2026-09-23)

**How it appeared**

A client on a NATS leaf node reaches the hub's JetStream only under `$JS.<domain>.API.`
(ZeBridge §10hl: every `$JS.API` request from a leaf gets NoResponders). `JetStream` built
every API subject from a `const default_api_prefix = "$JS.API."` at three sites —
`sendRequest`, and the pull consumer's `CONSUMER.MSG.NEXT` in `nextSubject` and `fetch` —
so a caller had no way to say which JetStream it meant. nats.go spells this `nats.Domain`,
nats.js `{ domain }` on `jetstream()`/`jetstreamManager()`.

**Change** (`nats.zig-jetstream-domain.patch`, 5 hunks, 1 file)

`src/jetstream.zig` — `domain: ?[]const u8 = null` on `JetStreamOptions`; a public
`JetStream.apiSubject(a, tail_fmt, args)` that renders `$JS.API.<tail>` or
`$JS.<domain>.API.<tail>`; the three sites call it. KV and object stores need nothing:
their `$KV.` and `$O.` subjects are data, not API, and their API calls already go through
`sendRequest`. Null keeps every existing caller on the plain prefix. One unit test.

**Verified**

```bash
cd nats.zig && zig build test-unit          # 135/135 (+1)
git -C nats.zig apply -R --check …          # reverses cleanly
```

Live, ZeBridge §10is: against a server with `jetstream { domain: hub }` and `-DV`, a
`JetStream` with `.domain = "hub"` published `PUB $JS.hub.API.STREAM.INFO.KV_tenants` and
got the hub's answer; with `.domain = "other"` the server returned `503` with
`Nats-Subject: $JS.other.API.STREAM.INFO.KV_tenants`.

---

## 16. The two fixes from before the ledger (2026-08-27, ledgered 2026-09-23)

Found by the regeneration in §17: two changes in the working tree that no patch carried
and no entry here described. Both are in the submodule's old in-tree `NOTES.md` (untracked,
from before this ledger moved to `nats.zig-patch/`), and both say "LOCAL FIX (see
NOTES.md)" at the code. Ledgered here from that file; the patches are the series' first two.

**`01-nats.zig-direct-get-subject-form.patch`** — `getMsgDirect` published every
request to the bucket-level API (`$JS.API.DIRECT.GET.<stream>`) with a JSON body. For a
`last_by_subj`-only lookup it uses the ADR-31 subject form — `DIRECT.GET.<stream>.<subject>`
with an empty body — and keeps the JSON form for `seq` and `next_by_subj`. The two forms
are identical for last-by-subject, but only the subject form works under grants that scope
direct gets PER KEY, which is what makes ZeBridge's tenant-scoped KV and object grants
possible at all. Measured 2026-08-27 against a live JWT-mode server: `KV.get` and the
object store's meta lookups failed with a Permissions Violation under client credentials
before, pass after.

**`02-nats.zig-consumer-create-named.patch`** — a consumer with a name went through the
legacy `CONSUMER.DURABLE.CREATE.<stream>.<name>`; it now uses the modern
`CONSUMER.CREATE.<stream>.<name>[.<filter>]` (server ≥ 2.9) whenever `name` or
`durable_name` is set. Least-privilege grants cover the modern form and typically not the
legacy one, and under JWT auth an unauthorised API publish is dropped, so the failure was a
bare `Timeout`. Measured the same day: durable pull consumers timed out under client
credentials before, create cleanly after.

---

## 17. The patches regenerated as an ordered series (2026-09-23)

**How it appeared**

Asked whether the patches were up to date, the honest check was run for the first time:
does clean upstream `d4cd40d` plus the patch files rebuild the working tree? It does not,
in any order. 7 of 15 never applied to clean upstream; unwinding from the working tree
freed 10 and left 5 interlocked (`inbox-prefix`, `shared-pull-inbox`, `connection`,
`quiet-deliberate-close`, `stale-408`); 14 pairs of patches carried each other's lines
(cut from a full `git diff` with hunks filtered by hand, as the workflow note warned);
the `tests/jetstream_pull_test.zig` change (+163, the fetch-contract tests of entry 7)
was in no patch; two fixes were in no patch and no entry (§16). `apply -R --check` had
passed for 13 of 15 all along — it passes for a patch whose lines a later patch also
carries, so it never proved anything.

**Change** (every patch file replaced; the working tree untouched)

The 83 zero-context hunks of the full working-tree diff were attributed to a topic by
their changed lines (twelve read by hand; two hunks staged, because a later topic rewrote
a line an earlier one added: `nextSubject` — `shared-pull-inbox`, then `jetstream-domain`
— and `inbox_base` — `shared-pull-inbox`, then `inbox-prefix`); seventeen states were
rebuilt from upstream by cumulative hunk subsets; each patch is the exact diff between two
consecutive states. Chronological order:

## 18. `fetch` waits for the batch when the caller says so (2026-09-24)

**How it appeared**

`fetch` returns 1 ms after its first message (§13's contract: one CDC event, at once).
An object reader pulls `batch` chunks that are contiguous and always all coming, and
over any real network they do not land within a millisecond of each other. ZeBridge's
libzb on an iPhone over Wi-Fi: `fetch(8)` returned with one chunk, the reader asked for
eight more while seven were still in flight, and so on — nats-server's `connz` showed
59 requests in a few seconds, 513 chunks delivered, 42 MB pending on the phone's
connection, then `Slow Consumer (Pending Bytes)` and the connection cut, three times
in a row, on a 100 MB object it never got a third of. Loopback never showed it: there
the eight chunks arrive inside the idle window every time.

**Change** (`18-nats.zig-fetch-idle-after-first.patch`, 2 hunks, 1 file)

`src/jetstream.zig` — `idle_after_first: Io.Timeout = fetch_idle_after_first` on
`PullSubscription`, and `fetch` reads it instead of the constant. Every existing caller
keeps the 1 ms; an object reader sets it to its fetch timeout and a fetch returns with
the batch, or the deadline. libzb's `ObjectPull` does (ZeBridge §10iy).

**Verified**

```bash
nats.zig-patch/new-patch.sh fetch-idle-after-first
# wrote 18-nats.zig-fetch-idle-after-first.patch (2 hunks, 1 files)
# ✅ 18 patches: upstream d4cd40d + series == working tree (src, tests)
```

Live: the iPhone seed that was cut three times ran through — ZeBridge §10iy has the
number.

| series | ledger | topic |
| --- | --- | --- |
| 01 | §16 | direct-get-subject-form (2026-08-27) |
| 02 | §16 | consumer-create-named (2026-08-27) |
| 03 | §1 | jetstream-stale-408 |
| 04 | §2 | connection (UNSUB on a closed connection) |
| 05 | §7 | fetch-early-return-503 — now carries its tests |
| 06 | §7 | consumer-inactive-threshold |
| 07 | §3, §7 | shared-pull-inbox |
| 08 | §5 | quiet-deliberate-close |
| 09 | §6 | auth-verdict |
| 10 | §8 | objstore-double-release |
| 11 | §9 | quiet-routine-connect |
| 12 | §10 | jetstream-error-level |
| 13 | §11 | pull-next-delivered |
| 14 | §12 | tls13-ciphers |
| 15 | §13 | inbox-prefix |
| 16 | §14 | objstore-max-age |
| 17 | §15 | jetstream-domain |
| 18 | §18 | fetch-idle-after-first |
| 19 | §19 | creds-content |

Hunk and file counts quoted in older entries describe the old files; the series' own
counts are in each file. `check-series.sh` and `new-patch.sh` are the workflow from here.

**Verified**

```bash
nats.zig-patch/check-series.sh
# ✅ 18 patches: upstream d4cd40d + series == working tree (src, tests)
nats.zig-patch/new-patch.sh probe
# nothing to cut: the working tree equals upstream + the series
```

Every intermediate state compiles and passes its unit tests — `zig build test-unit` after
each patch in turn: 132/132 through 14, 134/134 at 15 (inbox-prefix adds two), 135/135 at
17 (domain adds one). The first cut did not: states 07–14 failed on `self.nc.newInbox`,
which `inbox-prefix` introduces at 15 — the old `shared-pull-inbox.patch` had been re-cut
after inbox-prefix and carried the newer line; staging `inbox_base` fixed it, and that is
the kind of anachronism only a per-state build catches. One unit test is flaky: it failed
once in ~4–6 runs, at a different state each time and on the unchanged final tree too, so
it is not the series; six further runs passed and it never printed its name. Recorded, not
hidden.

## 19. Credentials from memory, not only from a file (2026-09-25)

**How it appeared**

ZeBridge gives both of its clients one option vocabulary (§10ja), and zb-client-ts
takes the credentials as text: a browser or a phone app holds what `/enroll` returned
and has no file to point at. libzb could only pass `user_creds`, a path, so the Flutter
and React Native apps wrote the creds to disk first just to have one. nats.zig parses a
.creds file's content already (`creds.parse`); only the option to hand it over was
missing.

**Change** (`19-nats.zig-creds-content.patch`, 4 hunks, 2 files)

`src/connection.zig` — `user_creds_content: ?[]const u8`: the content of a .creds file,
owned by the caller and kept alive for the connection (every handshake reads it). Used
when `user_creds` is not set, in both places `user_creds` is read: the up-front check
(a malformed content fails before dialing, like an unreadable file) and the handshake.

`tests/auth_test.zig` — "jwt credentials content authentication" (the fixture's bytes,
against the JWT server) and "malformed credentials content fails before connecting".

**Verified**

```bash
nats.zig-patch/new-patch.sh creds-content
# wrote 19-nats.zig-creds-content.patch (4 hunks, 2 files)
# ✅ 19 patches: upstream d4cd40d + series == working tree (src, tests)
```

`zig build test-unit`: 135/135 on the first run; on a second run one TLS reconnect test
timed out (a handshake timeout under load, nothing of this patch). The e2e suite did not
run: on OrbStack its `waitForHealthyServices` timed out at 10 s with all ten containers
reporting healthy, and `beforeAll`'s `docker compose up -d` started nothing until run by
hand — an environment problem, recorded, not fixed here. The two new tests are
therefore unrun; the live proof is ZeBridge's: libzb connecting to the dev stack with
`"creds": <bob.creds text>` synced as bob on tenant globex, and garbage content was
refused before any socket (`MissingUserJwt`).


## 20. A pull fetch asks only for what the inbox does not already hold (2026-09-26)

**How it appeared**

libzb's live tail, following a table written at 75k–100k events/s from an already-live
replica (ZeBridge §10jb), reported `SlowConsumer` from `PullInbox.fetch` in two runs of
four, each time followed by a gap and a re-seed. The inbox depth, traced before every
pull, explained it: 231–234 messages (~45 MB of 256 KB CDC messages) queued before 154
of 163 fetches. `fetch` returns once messages stop for `idle_after_first` (patch 18); the
server keeps sending the rest of that request into the inbox; the next fetch requested a
full batch on top of it. Steady state: more than two batches queued, a hair under the
subscription's 64 MB pending limit — and past that limit the connection DROPS the
message, which a JetStream reader can only see as a hole.

**Change** (`20-nats.zig-pull-deficit.patch`, 4 hunks, 1 file)

`src/jetstream.zig` — `PullSubscription.fetch` and `PullInbox.fetch` read the inbox
subscription's `pending_msgs` first and request `batch − queued` (the shared inbox splits
`queued` evenly over its consumers); when the queue alone fills the batch no request is
sent and the fetch returns from the queue. The collection loop's bound is the caller's
`batch`, which the queue's share and the request's share make up together. Status frames
count as queued too — harmless: the idle window returns the fetch as before.

**Verified**

```bash
nats.zig-patch/new-patch.sh pull-deficit
# wrote 20-nats.zig-pull-deficit.patch (4 hunks, 1 files)
# ✅ 20 patches: upstream d4cd40d + series == working tree (src, tests)
```

`zig build test-unit`: 135/135. The live proof is ZeBridge's firehose at 37.5k and 50k
rows/s with both clients live (§10jb): the inbox depth before a pull, the poll errors and
the re-seed count, before and after.

## 21. A pull request can cap its bytes (2026-09-26)

**How it appeared**

libzb on an iPhone over Wi-Fi: nats-server disconnected it 66 times as a "Slow Consumer
(Pending Bytes)". A pull request of 100 CDC messages is 20–25 MB; the server queues a
request's messages for the connection at once, and requests overlap (a fetch returns after
its first message, patch 18; the next poll asks again, patch 20 counting only what already
ARRIVED). Over a link of a few MB/s the queue passed the server's per-connection
`max_pending` (64 MB) and the server closed the connection — every delivery in flight lost,
re-sent only after `ack_wait`, out of order.

**Change** (`21-nats.zig-pull-max-bytes.patch`, 4 hunks, 1 file)

`src/jetstream.zig` — `PullSubscription.max_bytes` and `PullInbox.max_bytes` (default null:
unchanged), sent as the request's `max_bytes`. The server then stops a request at that many
bytes (409 "Message Size Exceeds MaxBytes" ends it, handled as before).

**Verified**: `zig build test-unit` 135/135; libzb sets 8 MB on its tail inbox and its drain
consumers (ZeBridge §10jc).

## 22. One outstanding pull request per consumer (2026-09-26)

**How it appeared**

Patch 21's byte cap bounded one request, not how many were open. `fetch` returns after its
first message (patch 18) and the next fetch asked again (patch 20 counts only what already
ARRIVED), so an iPhone polling once a second held a dozen 8 MB requests at once over Wi-Fi:
still 44 disconnects as a slow consumer after patch 21.

**Change** (`22-nats.zig-pull-one-request.patch`, src/jetstream.zig)

`PullSubscription` keeps its one outstanding request (reply subject, messages still owed,
deadline). `PullSubscription.fetch` and `PullInbox.fetch` send a new request only when the
last has delivered everything, ended with a status (404/408/409/503), or expired; data
messages are counted against the consumer their ack metadata names. Patch 20's deficit
arithmetic is gone with it (the inbox now only ever holds the current request's rest).

**Verified**: `zig build test-unit` 135/135; ZeBridge's delivery-loss test exact on the Mac
(138 simulated losses); the iPhone at 40k events/s with four SIGKILLs: 0 disconnects (the
server's slow-consumer count unchanged), 153 of 153 batches exact.

## 23. Up to two outstanding pull requests (2026-09-26)

**How it appeared**

With one request at a time (patch 22) a phone's catch-up alternated transfer and apply —
~1.5 s each per 8 MB on an iPhone 12 over Wi-Fi: a 1.3M-row first seed took 91.8 s.

**Change** (`23-nats.zig-pull-depth.patch`, src/jetstream.zig)

`PullSubscription.pull_depth` (default 1: unchanged; at most 2). The one tracked request
becomes two slots; a fetch issues requests while fewer than `pull_depth` are open; a data
message is owed by the OLDEST open request (the server serves a consumer's requests in
order); a status frame ends the request whose reply subject it names. `PullInbox` reply
subjects become `<prefix><fetch>x<i>y<k>`. Bytes in flight: at most `pull_depth × max_bytes`.

**Verified**: `zig build test-unit` 135/135 (three runs; one earlier run had the known flaky
TLS test fail); ZeBridge's delivery-loss test exact; the iPhone's 1.3M-row first seed
53.7 s (was 91.8 s), exact. One slow-consumer disconnect remained during that seed's
drain — being measured.

## 24. A pull request stays open while its messages arrive (2026-09-26)

**How it appeared**

After patch 23 one slow-consumer disconnect remained, during a phone's drain. A request's
slot freed at its `expires` (0.9 s for libzb's drain), but the server queues a request's
messages at once and 8 MB over ~5 MB/s Wi-Fi takes 1.6 s: the slot freed with data still on
the wire, the next fetch asked again, and the bytes in flight passed `pull_depth × max_bytes`.

**Change** (`24-nats.zig-pull-grace.patch`, src/jetstream.zig)

Each delivered message extends its request's deadline to at least now + `request_grace`
(2 s). A request frees when fully delivered, ended by a status (408 at expiry, 409 at the
byte cap), or silent for the grace after its expiry.

**Verified**: `zig build test-unit` 135/135; ZeBridge's delivery-loss test exact; the iPhone at
40k events/s with four SIGKILLs: 0 slow-consumer disconnects, 0 connection resets, 153/153
batches exact, converged 170 s after the load (285 s before patches 21-24).

## 25. It compiles for 32-bit ARM (2026-09-26)

**How it appeared**

A moto e20 (Android 11 Go, 1.8 GB) runs a 32-bit userspace (`abilist`: armeabi-v7a,
armeabi): Android loads the armeabi-v7a Flutter engine and needs a 32-bit libzb, which never
compiled — `std.atomic.Value(u64)` is refused on 32-bit ARM by Zig 0.16 whatever the CPU
("expected 32-bit integer type or smaller"), and three u64/usize mixes.

**Change** (`25-nats.zig-arm32.patch`)

`src/atomic_u64.zig` — `AtomicU64`: `std.atomic.Value(u64)` where usize is 64-bit, else a
spin-locked u64 with the four operations used (load, store, fetchAdd, fetchSub); the six
64-bit counters of `connection.zig` and `subscription.zig` use it. `nuid.zig`,
`jetstream_kv.zig`, `jetstream_objstore.zig`: explicit casts (an object larger than a 32-bit
address space fails as OutOfMemory). A test for the locked form.

**Verified**: `zig build test-unit` 136/136; libzb builds for `arm-linux-androideabi` with
`-Dcpu=cortex_a7` (libzb itself needed no change); the Flutter app starts on the moto e20.
