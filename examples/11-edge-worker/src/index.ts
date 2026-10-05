// A Cloudflare Worker asks the airports service over NATS: WebSocket to the hub, the
// principal's own inbox (the grants require it), one request, the answer and the times.
import { wsconnect, credsAuthenticator } from '@nats-io/nats-core';
import { decompress } from 'fzstd';

/// The service compresses its answers (zstd), as zb-client-ts's `maybeZstd` expects.
const unzstd = (b: Uint8Array) =>
  b.length >= 4 && b[0] === 0x28 && b[1] === 0xb5 && b[2] === 0x2f && b[3] === 0xfd ? decompress(b) : b;

interface Env { ZB_CREDS_B64: string; ZB_PRINCIPAL: string; ZB_NATS_WS_URL: string }

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const u = new URL(req.url);
    const lat = Number(u.searchParams.get('lat') ?? 50.11);
    const lng = Number(u.searchParams.get('lng') ?? 8.682);
    const t0 = Date.now();
    const nc = await wsconnect({
      servers: env.ZB_NATS_WS_URL,
      authenticator: credsAuthenticator(new TextEncoder().encode(atob(env.ZB_CREDS_B64))),
      inboxPrefix: `_INBOX.${env.ZB_PRINCIPAL}`,
      name: 'edge-worker',
    });
    const t1 = Date.now();
    try {
      const m = await nc.request('query._default.airports_near',
        new TextEncoder().encode(JSON.stringify({ lat, lng, radius_km: 100, limit: 5 })), { timeout: 5000 });
      const t2 = Date.now();
      const a = JSON.parse(new TextDecoder().decode(unzstd(m.data)));
      const code = a.columns?.indexOf('code');
      return Response.json({
        connect_ms: t1 - t0, request_ms: t2 - t1, service_ms: a.ms, count: a.count, bytes: m.data.length,
        airports: (a.rows ?? []).map((r: any[]) => r[code]),
        colo: (req as any).cf?.colo ?? 'local',
      });
    } catch (e) {
      return Response.json({ connect_ms: t1 - t0, error: String(e) }, { status: 502 });
    } finally {
      await nc.close();
    }
  },
};
