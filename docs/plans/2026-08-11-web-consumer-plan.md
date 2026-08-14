# Web Consumer Browser Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a clean, readable Vite + Vanilla TypeScript browser client in `/web-consumer` that connects to NATS JetStream via WebSockets (`ws://localhost:8080`), decodes `INIT` snapshots and `CDC` events (`INSERT`/`UPDATE`/`DELETE`), appends formatted event logs into an overflow scroll container, and includes a UI Mutation Panel to publish client-side fan-in mutations over WebSockets.

**Architecture:** Update NATS configuration to enable WebSockets on port 8080 and expose it in Docker Compose. Scaffold `/web-consumer` using Vite + pnpm, connect to NATS via `nats.ws`, decode MessagePack/JSON payloads, render a minimal UI log, and provide structured mutation form inputs for `users` and `test_types`.

**Tech Stack:** Vite, Vanilla TypeScript, `nats.ws`, `@msgpack/msgpack`, Docker Compose.

## Global Constraints

- NATS WebSocket port: `8080` (no TLS in development)
- NATS User: `${NATS_BRIDGE_USER}` (default: `bridge_user`)
- NATS Password: `${NATS_BRIDGE_PASSWORD}` (default: `bridge_secure_password`)
- Output path for web app: `/web-consumer`
- UI Styling: Minimalist, readable, un-cluttered CSS (no Tailwind required)

---

### Task 1: Enable NATS WebSockets in Configuration & Docker Compose

**Files:**
- Modify: `nats-server.conf.template:1-16`
- Modify: `docker-compose.full.yml:82-99`

- [x] **Step 1: Update `nats-server.conf.template` to enable WebSockets** (Completed)
- [x] **Step 2: Update `docker-compose.full.yml` to expose port 8080 on `nats-server`** (Completed)
- [x] **Step 3: Test Docker Compose configuration syntax** (Completed)
- [x] **Step 4: Commit Infrastructure Changes** (Completed)

---

### Task 2: Scaffold `/web-consumer` App Scaffolding

**Files:**
- Create: `web-consumer/package.json`
- Create: `web-consumer/index.html`
- Create: `web-consumer/src/style.css`
- Create: `web-consumer/vite.config.ts`

- [x] **Step 1: Create `web-consumer/package.json`** (Completed)
- [x] **Step 2: Create `web-consumer/index.html`** (Completed)
- [x] **Step 3: Create `web-consumer/src/style.css`** (Completed)
- [x] **Step 4: Create `web-consumer/vite.config.ts`** (Completed)
- [x] **Step 5: Install dependencies in `/web-consumer`** (Completed)
- [x] **Step 6: Commit Scaffolding** (Completed)

---

### Task 3: Implement NATS WebSocket Connection & JetStream Consumer Logic

**Files:**
- Create: `web-consumer/src/main.ts`

- [x] **Step 1: Create `web-consumer/src/main.ts`** (Completed)
- [x] **Step 2: Test TypeScript compilation** (Completed)
- [x] **Step 3: Commit Application Logic** (Completed)

---

### Task 4: Implement Client Mutation Panel (Fan-In UI & NATS Publish)

**Files:**
- Modify: `web-consumer/index.html`
- Modify: `web-consumer/src/style.css`
- Modify: `web-consumer/src/main.ts`

**Interfaces:**
- Consumes: User inputs for `users` and `test_types` tables
- Produces: Encoded MessagePack mutation payload published to `mutation.<table_name>.<operation>` with HLC timestamp

- [x] **Step 1: Update `web-consumer/index.html` to add Mutation Form Panel**

Add a `<section class="mutation-panel">` containing:
- Table selector (`users` vs `test_types`)
- Operation selector (`INSERT`, `UPDATE`, `DELETE`)
- Form fields for `users` (`id`, `name`, `email`)
- Form fields for `test_types` (`id`, `some_text`, `age`, `price`, `is_true`)
- "Publish Mutation" button

- [x] **Step 2: Update `web-consumer/src/style.css` for form inputs**

Add simple, readable grid styling for form fields and mutation section.

- [x] **Step 3: Update `web-consumer/src/main.ts` to process & publish mutation payloads**

1. Add event listeners for table selection change to toggle field visibility between `users` and `test_types`.
2. On clicking "Publish Mutation":
   - Generate HLC timestamp (`Date.now() + "-0001"`).
   - Generate `msg_id` (`mut-` + random string).
   - Construct structured mutation payload.
   - Encode payload via `@msgpack/msgpack` (with JSON fallback).
   - Publish to subject `mutation.<table_name>.<op>`.
   - Append log entry `[MUTATION OUT]` to the log window.

- [x] **Step 4: Verify TypeScript build**

Run: `cd web-consumer && pnpm run build`
Expected: Passes clean with 0 errors.

- [x] **Step 5: Commit Mutation Panel**

```bash
git add web-consumer/
git commit -m "feat(web-consumer): add fan-in mutation panel for users and test_types"
```
