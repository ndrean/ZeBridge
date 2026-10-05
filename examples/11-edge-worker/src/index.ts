// An HTTP front for ZeBridge's responders, at the edge. A client sends a plain HTTP request;
// the Worker hands it to one Durable Object, which holds a NATS connection open and asks
// `query.<tenant>.<name>` — no library, no enrollment, no NATS on the client's side.
//
//   POST /q/airports_near   {"lat":50.11,"lng":8.682}      (or GET /q/airports_near?lat=…&lng=…)
//
// The Worker keeps nothing between requests; the Durable Object does: the first request
// connects (WebSocket, TLS, the server's challenge signed with the seed), the next ones reuse
// the connection and cost the question alone.
import { DurableObject } from 'cloudflare:workers';
import { wsconnect, credsAuthenticator, type NatsConnection } from '@nats-io/nats-core';
import { decompress } from 'fzstd';

export interface Env {
  NATS: DurableObjectNamespace<NatsGateway>;
  ZB_CREDS_B64: string;       // the principal's creds (enroll.py), a secret
  ZB_PRINCIPAL: string;       // its name: replies come to _INBOX.<principal>, the grants allow no other
  ZB_NATS_WS_URL: string;     // the NATS WebSocket to dial
  ZB_TENANT?: string;         // whose services to ask; `_default`, the open tenant, when unset
}

/// A question's name, as a NATS subject token: nothing that could widen the subject.
const NAME = /^[A-Za-z0-9_]{1,64}$/;

/// The service compresses its answers (zstd), as zb-client-ts's `maybeZstd` expects.
const unzstd = (b: Uint8Array) =>
  b.length >= 4 && b[0] === 0x28 && b[1] === 0xb5 && b[2] === 0x2f && b[3] === 0xfd ? decompress(b) : b;

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const u = new URL(req.url);
    const m = /^\/q\/([^/]+)$/.exec(u.pathname);
    if (!m || !NAME.test(m[1])) return Response.json({ error: 'ask /q/<name>' }, { status: 404 });
    let payload: Record<string, unknown>;
    if (req.method === 'POST') {
      try { payload = await req.json(); } catch { return Response.json({ error: 'the body is not JSON' }, { status: 400 }); }
    } else {
      // GET: the query string, numbers as numbers.
      payload = Object.fromEntries([...u.searchParams].map(([k, v]) => [k, v !== '' && !Number.isNaN(Number(v)) ? Number(v) : v]));
    }
    const gateway = env.NATS.get(env.NATS.idFromName('nats'));
    const r = await gateway.ask(m[1], payload);
    return Response.json({ ...r, colo: (req as any).cf?.colo ?? 'local' }, { status: r.error ? 502 : 200 });
  },
};

/// One NATS connection for every request, held between them.
export class NatsGateway extends DurableObject<Env> {
  private nc: NatsConnection | null = null;
  private connecting: Promise<NatsConnection> | null = null;

  private async conn(): Promise<{ nc: NatsConnection; connect_ms: number }> {
    if (this.nc && !this.nc.isClosed()) return { nc: this.nc, connect_ms: 0 };
    const t0 = Date.now();
    this.connecting ??= wsconnect({
      servers: this.env.ZB_NATS_WS_URL,
      authenticator: credsAuthenticator(new TextEncoder().encode(atob(this.env.ZB_CREDS_B64))),
      inboxPrefix: `_INBOX.${this.env.ZB_PRINCIPAL}`,
      name: 'zb-edge-gateway',
    }).finally(() => { this.connecting = null; });
    this.nc = await this.connecting;
    return { nc: this.nc, connect_ms: Date.now() - t0 };
  }

  async ask(name: string, payload: unknown): Promise<Record<string, any>> {
    const tenant = this.env.ZB_TENANT || '_default';
    let connect_ms = 0;
    try {
      const c = await this.conn();
      connect_ms = c.connect_ms;
      const t0 = Date.now();
      const m = await c.nc.request(`query.${tenant}.${name}`, new TextEncoder().encode(JSON.stringify(payload ?? {})), { timeout: 5000 });
      const request_ms = Date.now() - t0;
      return { ...JSON.parse(new TextDecoder().decode(unzstd(m.data))), connect_ms, request_ms };
    } catch (e) {
      // A dropped connection is opened again on the next request.
      if (this.nc?.isClosed()) this.nc = null;
      return { error: String(e), connect_ms };
    }
  }
}
