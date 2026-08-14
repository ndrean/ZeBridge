# NATS JetStream & Zig Implementation Learnings

This document captures key technical details discovered during the implementation and debugging of the ZeBridge NATS integration.

### 1. JetStream Pull Consumer Memory Leaks (408 Request Timeout)
When a JetStream pull consumer asks for a message and times out (e.g. `consumer.CONSUME(timeout_ns)`), the NATS server responds with a `408 Request Timeout` message that has **no payload**. 
- In the `g41797/nats` library, skipping this message with a simple `continue` causes a memory leak because the message envelope is never returned to the allocator pool.
- **Fix:** You MUST explicitly call `consumer.REUSE(msg)` when encountering an empty payload or when skipping processing due to an error.

```zig
if (consumer.CONSUME(timeout_ns) catch null) |msg| {
    const payload = msg.letter.getPayload() orelse {
        consumer.REUSE(msg);
        continue;
    };
    // ...
}
```

### 2. Pull Consumer State Invalidation
The `mutation_listener` uses a stateful JetStream Pull Consumer (`nats.Consumer.START`). 
- If the NATS server restarts, the pull consumer's state on the server gets invalidated.
- The `CONSUME` loop in Zig will silently time out without throwing a fatal error. The bridge process must currently be restarted to re-initialize the consumer state.

### 3. Graceful Shutdown & Polling Timeouts
The consumers in `mutation_listener` and `snapshot_listener` call into the vendored library's `waitMessageNMT()` and `CONSUME()` functions. 
- Under the hood, this puts the thread to sleep using a low-level OS condition variable until a message arrives or the timeout expires.
- Because the thread is asleep deep inside the vendor library code, it cannot check application-level `should_stop` flags until it wakes up.
- **Fix:** Instead of exposing internal connection structs to trigger the library's internal `interrupt()` function on shutdown, dropping the polling timeout to `500ms` provides a near-instant shutdown without butchering the vendored library's internals or drastically increasing CPU usage.

### 4. Ephemeral Consumers in `nats.ws`
In newer versions of `nats.ws` (v1.29+), `js.consumers.get("STREAM", { filterSubjects: "..." })` silently fails to create an ephemeral consumer because the API changed.
- **Fix:** Use the JetStream Manager to explicitly add the ephemeral consumer first:
```typescript
const ci = await jsm.consumers.add("STREAM", { filter_subject: "..." });
const consumer = await js.consumers.get("STREAM", ci.name);
```
