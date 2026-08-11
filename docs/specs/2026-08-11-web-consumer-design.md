# Design Spec: Web Consumer Browser Client (`/web-consumer`)

## Objective
Create a simple, readable browser-based client in `/web-consumer` using Vite, Vanilla JS/TS, `nats.ws`, and `@msgpack/msgpack`. The client connects to NATS JetStream over WebSockets, decodes `INIT` snapshots and `CDC` events (`INSERT`/`UPDATE`/`DELETE`), and appends formatted event logs into an overflow scroll container in the browser.

*(Note: WASM SQLite migrations, table creations, and client-side mutations are deferred to Phase 2. Phase 1 is strictly logging and event streaming).*

---

## 1. Infrastructure Updates

### NATS Server Configuration (`nats-server.conf.template`)
Enable WebSockets on port 8080:
```conf
websocket {
  port: 8080
  no_tls: true
}
```

### Docker Compose (`docker-compose.full.yml`)
Expose port `8080` for `nats-server`:
```yaml
ports:
  - "4222:4222"
  - "8222:8222"
  - "8080:8080"
```

---

## 2. Web Consumer Application (`/web-consumer`)

### Technology Stack
- **Framework/Bundler**: Vite + Vanilla TypeScript
- **NATS Connection**: `nats.ws`
- **Decoding**: `@msgpack/msgpack` (MessagePack) with JSON fallback

### UI Layout (Minimal & Readable)
- **Header**: Connection status badge (`Connected` / `Disconnected`), NATS server URL (`ws://localhost:8080`).
- **Action Controls**:
  - `Fetch Schemas`: Query NATS KV bucket `schemas`.
  - `Request Snapshot`: Publish to `snapshot.request.test_types` / `users`.
  - `Clear Logs`: Flush log container.
- **Log Output**: Clean overflow scroll container (`<div id="log-output">`) rendering incoming `INIT` snapshots and `CDC` `INSERT`/`UPDATE`/`DELETE` events.

---

## 3. Data Flow

1. Browser connects to NATS via `ws://localhost:8080` using credentials `bridge_user` / `bridge_secure_password`.
2. Subscribes to `init.>` (INIT stream) and `cdc.>` (CDC stream).
3. Decodes binary MessagePack payloads via `@msgpack/msgpack` (or JSON fallback).
4. Formats and appends structured event log entries to the UI log output.
